// Package sim replays one job stream against two admission policies on synthetic hosts, in
// memory, one simulated second at a time. It uses sched's own budget arithmetic for the
// reservation policy, so the decision under test is the real one.
//
// The model is deliberately small. A job ramps its CPU use from zero to its true peak over
// RampS seconds of its own progress. When a host's demand exceeds its cores, every job on it
// slows in proportion. Memory is reserved but not simulated as use, and nothing crashes: the
// numbers show overload, not its consequences.
package sim

import (
	"fmt"
	"math"
	"sort"

	"github.com/unlicensedthought/agent-fleet-scheduler/sched"
)

// Host is one synthetic machine.
type Host struct {
	Name          string  `json:"name"`
	NCPU          int     `json:"ncpu"`
	MemMB         int64   `json:"mem_mb"`
	NonFleetCores float64 `json:"non_fleet_cores"`
}

// Job is one synthetic job. Class is what it declares; PeakCores is what it really uses.
type Job struct {
	ID        string  `json:"id"`
	Class     string  `json:"class"`
	ArriveS   float64 `json:"arrive_s"`
	PeakCores float64 `json:"peak_cores"`
	RampS     float64 `json:"ramp_s"`
	RunS      float64 `json:"run_s"` // seconds of progress needed at full speed
}

// Scenario is a host set and a job stream.
type Scenario struct {
	Hosts []Host `json:"hosts"`
	Jobs  []Job  `json:"jobs"`
}

// Policy names.
const (
	Load    = "load"    // admit while the host's current load is under LoadFrac of its cores
	Reserve = "reserve" // admit against reserved peaks
)

// LoadFrac is the instantaneous-load policy's threshold: admit while load is under this
// fraction of the cores.
const LoadFrac = 0.8

// HostResult is one host's outcome.
type HostResult struct {
	Name        string  `json:"name"`
	NCPU        int     `json:"ncpu"`
	PeakDemand  float64 `json:"peak_demand"`
	OverSeconds int     `json:"over_seconds"` // seconds with demand above the core count
}

// Result is one policy's outcome on one scenario.
type Result struct {
	Policy     string       `json:"policy"`
	Makespan   int          `json:"makespan_s"`
	MeanWaitS  float64      `json:"mean_wait_s"`
	MaxAtOnce  int          `json:"max_jobs_at_once"`
	Hosts      []HostResult `json:"hosts"`
	Unfinished int          `json:"unfinished"`
}

type run struct {
	Job
	host    int
	state   int // 0 waiting, 1 queued on a host, 2 running, 3 done
	prog    float64
	started int
	wall    int // seconds running
	reserve sched.Reservation
}

// Simulate runs the scenario under a policy for at most maxS simulated seconds.
func Simulate(sc Scenario, policy string, maxS int) (Result, error) {
	classes := sched.DefaultClasses()
	jobs := make([]*run, len(sc.Jobs))
	for i, j := range sc.Jobs {
		if _, ok := classes[j.Class]; !ok {
			return Result{}, fmt.Errorf("job %s: unknown class %q", j.ID, j.Class)
		}
		jobs[i] = &run{Job: j, host: -1}
	}
	sort.SliceStable(jobs, func(a, b int) bool { return jobs[a].ArriveS < jobs[b].ArriveS })
	res := Result{Policy: policy}
	for _, h := range sc.Hosts {
		res.Hosts = append(res.Hosts, HostResult{Name: h.Name, NCPU: h.NCPU})
	}
	cores := func(j *run) float64 {
		if j.RampS <= 0 {
			return j.PeakCores
		}
		return j.PeakCores * math.Min(1, j.prog/j.RampS)
	}
	demand := func(h int) float64 {
		d := sc.Hosts[h].NonFleetCores
		for _, j := range jobs {
			if j.state == 2 && j.host == h {
				d += cores(j)
			}
		}
		return d
	}
	budget := func(h int) *sched.Budget {
		H := sc.Hosts[h]
		b := sched.NewBudget(H.Name, sched.Capacity{NCPU: H.NCPU, MemTotalMB: H.MemMB, MemAvailMB: -1, Tokens: map[string]int{}},
			sched.Proc{Cores: H.NonFleetCores}, classes)
		for _, j := range jobs {
			if (j.state == 2) && j.host == h {
				b.Take(j.reserve)
			}
		}
		for _, j := range jobs {
			if j.state == 1 && j.host == h {
				b.Queue = append(b.Queue, sched.QueueEntry{ID: j.ID, Cores: classes[j.Class].Cores})
			}
		}
		return b
	}
	start := func(j *run, t int) {
		c := classes[j.Class]
		j.state, j.started, j.wall = 2, t, 0
		j.reserve = sched.Reservation{ID: j.ID, Class: j.Class, Cores: c.Cores, MemMB: c.MemMB, Started: int64(t)}
	}
	var waitSum float64
	for t := 0; t <= maxS; t++ {
		switch policy {
		case Reserve:
			// arrivals: place on a host, start now if it fits, else join that host's queue
			for _, j := range jobs {
				if j.state == 0 && j.ArriveS <= float64(t) {
					bs := make([]*sched.Budget, len(sc.Hosts))
					for h := range bs {
						bs[h] = budget(h)
					}
					c := classes[j.Class]
					h := sched.Place(bs, sched.Need{Class: j.Class, Cores: c.Cores, MemMB: c.MemMB})
					if h < 0 {
						return res, fmt.Errorf("job %s fits no host", j.ID)
					}
					j.host, j.state = h, 1
				}
			}
			// each host admits its queue in arrival order while the head fits
			for h := range sc.Hosts {
				for {
					var head *run
					for _, j := range jobs {
						if j.state == 1 && j.host == h {
							head = j
							break
						}
					}
					if head == nil {
						break
					}
					c := classes[head.Class]
					if ok, _ := budget(h).Fits(sched.Need{Class: head.Class, Cores: c.Cores, MemMB: c.MemMB}); !ok {
						break
					}
					start(head, t)
				}
			}
		case Load:
			for _, j := range jobs {
				if j.state != 0 || j.ArriveS > float64(t) {
					continue
				}
				best, bestL := -1, 1e9
				for h, H := range sc.Hosts {
					if l := demand(h); l < LoadFrac*float64(H.NCPU) && l/float64(H.NCPU) < bestL {
						best, bestL = h, l/float64(H.NCPU)
					}
				}
				if best >= 0 {
					j.host = best
					start(j, t)
				}
			}
		default:
			return res, fmt.Errorf("unknown policy %q", policy)
		}
		live, rest := 0, 0
		for h := range sc.Hosts {
			d := demand(h)
			hr := &res.Hosts[h]
			hr.PeakDemand = math.Max(hr.PeakDemand, round1(d))
			if d > float64(sc.Hosts[h].NCPU)+1e-9 {
				hr.OverSeconds++
			}
			rate := math.Min(1, float64(sc.Hosts[h].NCPU)/math.Max(d, 1e-9))
			for _, j := range jobs {
				if j.state != 2 || j.host != h {
					continue
				}
				live++
				j.prog += rate
				j.wall++
				if policy == Reserve && j.wall >= int(sched.RaiseAfter.Seconds()) {
					// growth: raise the reservation to the measured peak, as the real scheduler does
					peak := math.Min(math.Ceil(cores(j)), float64(sc.Hosts[h].NCPU)-sched.OSReserveCores)
					if peak > j.reserve.Cores {
						j.reserve.Cores = peak
					}
				}
				if j.prog >= j.RunS {
					j.state = 3
					res.Makespan = t
					waitSum += float64(j.started) - j.ArriveS
				}
			}
		}
		if live > res.MaxAtOnce {
			res.MaxAtOnce = live
		}
		for _, j := range jobs {
			if j.state != 3 {
				rest++
			}
		}
		if rest == 0 {
			break
		}
	}
	for _, j := range jobs {
		if j.state != 3 {
			res.Unfinished++
		}
	}
	if done := len(jobs) - res.Unfinished; done > 0 {
		res.MeanWaitS = round1(waitSum / float64(done))
	}
	return res, nil
}

func round1(f float64) float64 { return math.Round(f*10) / 10 }
