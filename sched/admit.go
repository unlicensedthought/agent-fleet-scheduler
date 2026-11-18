package sched

import (
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
)

// Admit is the one admission decision. Under the lock it reaps the dead, measures, and either
// writes the reservation (it fits and nobody of equal or higher priority is waiting) or the queue
// entry. A job that fits but has equal-or-higher-priority work queued ahead of it waits too: the
// head of the queue is never overtaken by a smaller job behind it.
func (s *Store) Admit(req AdmitRequest) (AdmitResult, error) {
	unlock, err := s.Lock()
	if err != nil {
		return AdmitResult{}, err
	}
	defer unlock()
	s.reapLocked()
	b, err := s.Measure()
	if err != nil {
		return AdmitResult{}, err
	}
	out := AdmitResult{ID: req.ID, Host: b.Host}
	n, err := b.NeedFor(req.Class, req.Cores, req.MemMB)
	if err != nil {
		out.Refused = err.Error()
		return out, nil
	}
	out.Cores, out.MemMB = n.Cores, n.MemMB
	if why := b.NeverFits(n); why != "" {
		out.Refused = why
		s.Event("never_fits", map[string]any{"id": req.ID, "class": req.Class, "why": why})
		return out, nil
	}
	for _, r := range b.Reservations {
		if r.ID == req.ID {
			out.Admitted = true
			return out, nil
		}
	}
	ahead := 0
	for _, e := range b.Queue {
		if e.ID == req.ID {
			out.Queued, out.Position, out.Depth = true, e.Position, len(b.Queue)
			return out, nil
		}
		if e.Priority >= req.Priority {
			ahead++
		}
	}
	ok, why := b.Fits(n)
	if b.OverBudget {
		s.overBudget(b)
	}
	if ok && ahead == 0 {
		r := Reservation{ID: req.ID, Class: req.Class, Cores: n.Cores, MemMB: n.MemMB, Tokens: n.Tokens,
			PID: req.PID, Started: s.now().Unix()}
		if err := s.putReservation(r); err != nil {
			return out, err
		}
		s.Event("admit", map[string]any{"id": r.ID, "class": r.Class, "cores": r.Cores, "mem_mb": r.MemMB,
			"free_cores_after": round1(b.FreeCores - r.Cores), "free_mem_mb_after": b.FreeMemMB - r.MemMB})
		out.Admitted = true
		return out, nil
	}
	if ok {
		why = fmt.Sprintf("%d job(s) of equal or higher priority are queued ahead", ahead)
	}
	out.Reason = why
	if !req.Queue {
		return out, nil
	}
	e := QueueEntry{ID: req.ID, Class: req.Class, Priority: req.Priority, Seq: s.now().UnixNano(),
		Cores: n.Cores, MemMB: n.MemMB, Tokens: n.Tokens, PID: req.Waiter, LastWarn: s.now().Unix()}
	if err := s.putQueue(e); err != nil {
		return out, err
	}
	q, _ := s.Queue()
	for _, x := range q {
		if x.ID == e.ID {
			out.Position = x.Position
		}
	}
	out.Queued, out.Depth = true, len(q)
	s.Event("queue", map[string]any{"id": e.ID, "class": e.Class, "priority": e.Priority,
		"position": out.Position, "depth": out.Depth, "why": why})
	return out, nil
}

func (s *Store) overBudget(b *Budget) {
	s.Event("over_budget", map[string]any{"free_cores": b.FreeCores, "free_mem_mb": b.FreeMemMB,
		"non_fleet_cores": b.NonFleetCores, "non_fleet_mem_mb": b.NonFleetMemMB,
		"reserved_cores": b.ReservedCores, "reserved_mem_mb": b.ReservedMemMB})
}

// Step is one scheduler pass: reap, measure each reservation's process tree (raising one whose
// measured peak outgrew its class), then admit queue heads in order while they fit. Strict
// priority-then-FIFO: a head that does not fit blocks the ones behind it, so nothing starves.
// It returns the reservations it just granted from the queue; the caller starts those jobs.
func (s *Store) Step() ([]Reservation, error) {
	unlock, err := s.Lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	s.reapLocked()
	b, err := s.Measure()
	if err != nil {
		return nil, err
	}
	now := s.now()
	for i := range b.Reservations {
		r := &b.Reservations[i]
		u := b.usage[r.ID]
		changed := false
		if u.Cores > r.PeakCores {
			r.PeakCores, changed = round1(u.Cores), true
		}
		if int64(u.MemMB) > r.PeakMemMB {
			r.PeakMemMB, changed = int64(u.MemMB), true
		}
		if now.Sub(timeUnix(r.Started)) >= RaiseAfter && (r.PeakCores > r.Cores || r.PeakMemMB > r.MemMB) {
			oldC, oldM := r.Cores, r.MemMB
			r.Cores = math.Max(r.Cores, math.Min(math.Ceil(r.PeakCores), b.CapCores))
			r.MemMB = max(r.MemMB, min(roundMem(r.PeakMemMB), b.CapMemMB))
			if r.Cores != oldC || r.MemMB != oldM {
				r.Raised, changed = true, true
				b.ReservedCores += r.Cores - oldC
				b.ReservedMemMB += r.MemMB - oldM
				s.Event("over_reservation", map[string]any{"id": r.ID, "class": r.Class,
					"reserved_cores": oldC, "reserved_mem_mb": oldM, "peak_cores": r.PeakCores, "peak_mem_mb": r.PeakMemMB,
					"raised_to_cores": r.Cores, "raised_to_mem_mb": r.MemMB})
			}
		}
		if changed {
			if err := s.putReservation(*r); err != nil {
				s.warn(fmt.Sprintf("[sched] ERROR: updating reservation %s: %v", r.ID, err))
			}
		}
	}
	b.recompute()
	if b.OverBudget {
		s.overBudget(b)
	}
	var granted []Reservation
	for _, e := range b.Queue {
		n := Need{Class: e.Class, Cores: e.Cores, MemMB: e.MemMB, Tokens: e.Tokens}
		ok, why := b.Fits(n)
		if !ok {
			if now.Unix()-e.LastWarn >= int64(QueueWarnEvery.Seconds()) {
				e.LastWarn = now.Unix()
				_ = s.putQueue(e)
				s.Event("queue_wait", map[string]any{"id": e.ID, "class": e.Class, "position": e.Position,
					"waited_s": now.Unix() - e.Seq/1e9, "why": why})
			}
			break
		}
		r := Reservation{ID: e.ID, Class: e.Class, Cores: e.Cores, MemMB: e.MemMB, Tokens: e.Tokens,
			PID: e.PID, Started: now.Unix()}
		if err := s.putReservation(r); err != nil {
			return granted, err
		}
		s.dropQueue(e.ID)
		b.Take(r)
		s.Event("admit", map[string]any{"id": r.ID, "class": r.Class, "cores": r.Cores, "mem_mb": r.MemMB,
			"from_queue": true, "waited_s": now.Unix() - e.Seq/1e9})
		granted = append(granted, r)
	}
	if snap, err := json.Marshal(b); err == nil {
		if err := writeAtomic(filepath.Join(s.Dir, "budget.json"), snap); err != nil {
			s.warn(fmt.Sprintf("[sched] ERROR: writing budget snapshot: %v", err))
		}
	}
	return granted, nil
}

// Release drops a reservation (recording its peak) under the lock.
func (s *Store) Release(id string) (bool, error) {
	unlock, err := s.Lock()
	if err != nil {
		return false, err
	}
	defer unlock()
	res, _ := s.Reservations()
	for _, r := range res {
		if r.ID == id {
			s.dropReservation(id)
			s.RecordPeak(r)
			s.Event("release", map[string]any{"id": id, "class": r.Class, "peak_cores": r.PeakCores, "peak_mem_mb": r.PeakMemMB})
			return true, nil
		}
	}
	return false, nil
}

// Dequeue removes a queued entry; false when it is not queued (started, or never was).
func (s *Store) Dequeue(id, why string) (bool, error) {
	unlock, err := s.Lock()
	if err != nil {
		return false, err
	}
	defer unlock()
	if s.dropQueue(id) {
		s.Event("dequeue", map[string]any{"id": id, "why": why})
		return true, nil
	}
	return false, nil
}
