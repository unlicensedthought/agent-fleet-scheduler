package sched

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Store is one host's admission state on disk:
//
//	lock                 flock taken for every admission, release, reap and queue move
//	reservations/<id>    one per admitted job, held for its life
//	queue/<id>           one per waiting job (priority desc, then FIFO)
//	history.json         measured peaks per class; the class default follows them
//	events.jsonl         structured log of every admit/queue/reap/over-* decision
type Store struct {
	Dir     string
	Host    string
	Sampler Sampler
	Classes map[string]Class // base classes; DefaultClasses when nil
	Now     func() time.Time // time.Now when nil
	Stderr  func(string)     // warning sink; os.Stderr when nil
}

// Open creates the state directories and returns a store.
func Open(dir, host string, s Sampler) (*Store, error) {
	st := &Store{Dir: dir, Host: host, Sampler: s}
	for _, d := range []string{"reservations", "queue"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			return nil, err
		}
	}
	return st, nil
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Store) base() map[string]Class {
	if s.Classes != nil {
		return s.Classes
	}
	return DefaultClasses()
}

func (s *Store) warn(line string) {
	if s.Stderr != nil {
		s.Stderr(line)
		return
	}
	fmt.Fprintln(os.Stderr, line)
}

// Lock takes the host-wide admission lock. Every read-decide-write of reservations or the queue
// happens under it, so two spawns in the same second cannot both pass.
func (s *Store) Lock() (func(), error) {
	f, err := os.OpenFile(filepath.Join(s.Dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("flock: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// Event appends one structured line to events.jsonl and, for warnings, to stderr.
func (s *Store) Event(kind string, fields map[string]any) {
	fields["event"] = kind
	fields["ts"] = s.now().UTC().Format(time.RFC3339)
	fields["host"] = s.Host
	b, _ := json.Marshal(fields)
	f, err := os.OpenFile(filepath.Join(s.Dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		s.warn(fmt.Sprintf("[sched] ERROR: cannot write events.jsonl: %v; event was %s", err, b))
		return
	}
	_, _ = f.Write(append(b, '\n'))
	f.Close()
	switch kind {
	case "over_reservation", "over_budget", "reap", "queue_wait", "never_fits", "queue_dropped":
		s.warn("[sched] " + string(b))
	}
}

// Events returns the raw events.jsonl.
func (s *Store) Events() string {
	b, _ := os.ReadFile(filepath.Join(s.Dir, "events.jsonl"))
	return string(b)
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSONDir[T any](dir string, warn func(string)) ([]T, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []T
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue // removed between ReadDir and ReadFile
		}
		var v T
		if err := json.Unmarshal(b, &v); err != nil {
			warn(fmt.Sprintf("[sched] ERROR: unreadable %s: %v", filepath.Join(dir, e.Name()), err))
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

// Reservations lists the held reservations.
func (s *Store) Reservations() ([]Reservation, error) {
	return readJSONDir[Reservation](filepath.Join(s.Dir, "reservations"), s.warn)
}

// Queue lists waiting jobs: priority descending, then arrival order.
func (s *Store) Queue() ([]QueueEntry, error) {
	q, err := readJSONDir[QueueEntry](filepath.Join(s.Dir, "queue"), s.warn)
	sort.SliceStable(q, func(i, j int) bool {
		if q[i].Priority != q[j].Priority {
			return q[i].Priority > q[j].Priority
		}
		return q[i].Seq < q[j].Seq
	})
	for i := range q {
		q[i].Position = i + 1
	}
	return q, err
}

func (s *Store) putReservation(r Reservation) error {
	b, _ := json.MarshalIndent(r, "", "  ")
	return writeAtomic(filepath.Join(s.Dir, "reservations", r.ID+".json"), b)
}

func (s *Store) putQueue(q QueueEntry) error {
	b, _ := json.MarshalIndent(q, "", "  ")
	return writeAtomic(filepath.Join(s.Dir, "queue", q.ID+".json"), b)
}

func (s *Store) dropReservation(id string) bool {
	return os.Remove(filepath.Join(s.Dir, "reservations", id+".json")) == nil
}

func (s *Store) dropQueue(id string) bool {
	return os.Remove(filepath.Join(s.Dir, "queue", id+".json")) == nil
}

// HasReservation says whether id holds one.
func (s *Store) HasReservation(id string) bool {
	_, err := os.Stat(filepath.Join(s.Dir, "reservations", id+".json"))
	return err == nil
}

// pidAlive is a variable so tests can supply a process table without depending on host PIDs.
var pidAlive = func(pid int) bool {
	if pid <= 1 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// gone says why a reservation's holder no longer exists, or "" while it lives.
func (s *Store) gone(r Reservation) string {
	if r.PID == 0 {
		if s.now().Sub(time.Unix(r.Started, 0)) < StartGrace {
			return ""
		}
		return "never started (no pid)"
	}
	if !pidAlive(r.PID) {
		return fmt.Sprintf("pid %d is gone", r.PID)
	}
	return ""
}

// Attach records the holder pid of a reservation admitted before its job started.
func (s *Store) Attach(id string, pid int) error {
	unlock, err := s.Lock()
	if err != nil {
		return err
	}
	defer unlock()
	res, _ := s.Reservations()
	for _, r := range res {
		if r.ID == id {
			r.PID = pid
			return s.putReservation(r)
		}
	}
	return fmt.Errorf("%s holds no reservation", id)
}

func (s *Store) history() History {
	h := History{}
	if b, err := os.ReadFile(filepath.Join(s.Dir, "history.json")); err == nil {
		if err := json.Unmarshal(b, &h); err != nil {
			s.warn(fmt.Sprintf("[sched] ERROR: history.json unreadable (%v); class defaults fall back to base", err))
			return History{}
		}
	}
	return h
}

// RecordPeak appends a finished job's measured peak to its class's history.
func (s *Store) RecordPeak(r Reservation) {
	if r.PeakCores == 0 && r.PeakMemMB == 0 {
		return
	}
	h := s.history()
	h[r.Class] = append(h[r.Class], PeakRow{r.PeakCores, r.PeakMemMB, s.now().Unix()})
	if n := len(h[r.Class]); n > HistoryKeep {
		h[r.Class] = h[r.Class][n-HistoryKeep:]
	}
	b, _ := json.MarshalIndent(h, "", "  ")
	if err := writeAtomic(filepath.Join(s.Dir, "history.json"), b); err != nil {
		s.warn(fmt.Sprintf("[sched] ERROR: writing history.json: %v", err))
	}
}

// Measure builds the budget: capacity minus non-fleet use, minus reservations.
func (s *Store) Measure() (*Budget, error) {
	c, err := s.Sampler.Capacity()
	if err != nil {
		return nil, fmt.Errorf("host capacity: %w", err)
	}
	res, err := s.Reservations()
	if err != nil {
		return nil, err
	}
	q, err := s.Queue()
	if err != nil {
		return nil, err
	}
	procs, busy, err := s.Sampler.Procs()
	if err != nil {
		return nil, fmt.Errorf("process sample: %w", err)
	}
	roots := map[string]int{}
	for _, r := range res {
		roots[r.ID] = r.PID
	}
	delete(procs, os.Getpid()) // the measurement is not load
	per, rest := treeUsage(procs, roots)
	if busy >= 0 {
		var ours float64
		for _, u := range per {
			ours += u.Cores
		}
		rest.Cores = math.Max(0, busy-ours)
	}
	if c.MemAvailMB >= 0 {
		// Memory in use as the kernel counts it, less what the reservation trees hold: per-process
		// RSS cannot see wired or compressed memory.
		var ours float64
		for _, u := range per {
			ours += u.MemMB
		}
		rest.MemMB = math.Max(0, float64(c.MemTotalMB-c.MemAvailMB)-ours)
	}
	b := NewBudget(s.Host, c, rest, EffectiveClasses(s.base(), s.history(), float64(c.NCPU)-OSReserveCores, c.MemTotalMB-int64(math.Max(1024, float64(c.MemTotalMB)*OSReserveMemPct))))
	b.Queue, b.usage = q, per
	for i := range res {
		u := per[res[i].ID]
		res[i].NowCores, res[i].NowMemMB = round1(u.Cores), int64(u.MemMB)
		b.ReservedCores += res[i].Cores
		b.ReservedMemMB += res[i].MemMB
		for _, t := range res[i].Tokens {
			b.TokensHeld[t]++
		}
	}
	b.Reservations = res
	b.recompute()
	return b, nil
}

// reapLocked releases every reservation whose holder is gone and drops queued waiters whose
// process died. Caller holds the lock.
func (s *Store) reapLocked() {
	res, _ := s.Reservations()
	for _, r := range res {
		if why := s.gone(r); why != "" {
			s.dropReservation(r.ID)
			s.RecordPeak(r)
			s.Event("reap", map[string]any{"id": r.ID, "class": r.Class, "why": why, "cores": r.Cores, "mem_mb": r.MemMB,
				"peak_cores": r.PeakCores, "peak_mem_mb": r.PeakMemMB})
		}
	}
	q, _ := s.Queue()
	for _, e := range q {
		if e.PID > 0 && !pidAlive(e.PID) {
			s.dropQueue(e.ID)
			s.Event("queue_dropped", map[string]any{"id": e.ID, "why": fmt.Sprintf("waiter pid %d is gone", e.PID)})
		}
	}
}
