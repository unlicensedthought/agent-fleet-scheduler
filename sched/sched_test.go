package sched

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func newStore(t *testing.T, ncpu int, memMB int64, tokens map[string]int, nonFleet float64) (*Store, *Static) {
	t.Helper()
	smp := &Static{NCPU: ncpu, MemTotalMB: memMB, Tokens: tokens, NonFleetCores: nonFleet}
	s, err := Open(t.TempDir(), "a", smpAdapter{smp})
	if err != nil {
		t.Fatal(err)
	}
	s.Stderr = func(string) {}
	return s, smp
}

// smpAdapter lets a test change the sampled host after the store is open.
type smpAdapter struct{ s *Static }

func (a smpAdapter) Capacity() (Capacity, error)           { return a.s.Capacity() }
func (a smpAdapter) Procs() (map[int]Proc, float64, error) { return a.s.Procs() }

func req(id, class string, prio int) AdmitRequest {
	return AdmitRequest{ID: id, Class: class, Priority: prio, Queue: true}
}

// Two build jobs arrive in the same instant at a host that fits one. The host lock serialises the
// decisions: exactly one is admitted and the other queues, with the shortfall named.
func TestConcurrentAdmissionsOnlyOneFits(t *testing.T) {
	s, _ := newStore(t, 6, 16384, nil, 0) // cap 5 cores: one build (4) fits, two do not
	var wg sync.WaitGroup
	res := make([]AdmitResult, 2)
	for i := range res {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := s.Admit(req("job-"+strconv.Itoa(i), "build", 50))
			if err != nil {
				t.Error(err)
			}
			res[i] = r
		}(i)
	}
	wg.Wait()
	admitted, queued := 0, 0
	for _, r := range res {
		if r.Admitted {
			admitted++
		}
		if r.Queued {
			queued++
			if r.Position != 1 || !strings.Contains(r.Reason, "cores: need 4.0") {
				t.Fatalf("queued result %+v; want position 1 and a cores shortfall", r)
			}
		}
	}
	if admitted != 1 || queued != 1 {
		t.Fatalf("admitted %d queued %d; want 1 and 1: %+v", admitted, queued, res)
	}
}

// The same race across processes, not goroutines: the flock is on a file, so it must hold.
func TestConcurrentAdmitProcessesOnlyOneFits(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fleetctl")
	if out, err := exec.Command("go", "build", "-o", bin, "../cmd/fleetctl").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	st := t.TempDir()
	var wg sync.WaitGroup
	outs := make([]string, 4)
	for i := range outs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := exec.Command(bin, "admit", "--id", "p-"+strconv.Itoa(i), "--class", "build", "--queue")
			c.Env = append(os.Environ(), "FLEET_HOST_STATE="+st, "FLEET_TEST_HOST=6,16384")
			b, err := c.Output()
			if err != nil {
				t.Error(err)
			}
			outs[i] = string(b)
		}(i)
	}
	wg.Wait()
	joined := strings.Join(outs, "")
	if strings.Count(joined, `"admitted":true`) != 1 || strings.Count(joined, `"queued":true`) != 3 {
		t.Fatalf("want one admitted and three queued:\n%s", joined)
	}
}

func TestReleaseStartsTheQueuedOne(t *testing.T) {
	s, _ := newStore(t, 6, 16384, nil, 0)
	if r, _ := s.Admit(req("a", "build", 50)); !r.Admitted {
		t.Fatalf("first not admitted: %+v", r)
	}
	if r, _ := s.Admit(req("b", "build", 50)); !r.Queued {
		t.Fatalf("second not queued: %+v", r)
	}
	if g, _ := s.Step(); len(g) != 0 {
		t.Fatalf("granted %v while the first job still holds the budget", g)
	}
	if ok, err := s.Release("a"); !ok || err != nil {
		t.Fatalf("release: %v %v", ok, err)
	}
	g, err := s.Step()
	if err != nil || len(g) != 1 || g[0].ID != "b" {
		t.Fatalf("granted %v (%v); want b", g, err)
	}
	if s.HasReservation("a") || !s.HasReservation("b") {
		t.Fatal("a must be released and b held")
	}
}

// A holder that dies without releasing must not keep its reservation.
func TestKilledHolderIsReaped(t *testing.T) {
	s, _ := newStore(t, 10, 16384, nil, 0)
	c := exec.Command("sleep", "30")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Admit(AdmitRequest{ID: "victim", Class: "build", PID: c.Process.Pid, Queue: true}); !r.Admitted {
		t.Fatalf("not admitted: %+v", r)
	}
	if _, err := s.Step(); err != nil || !s.HasReservation("victim") {
		t.Fatalf("live holder's reservation dropped (err %v)", err)
	}
	_ = c.Process.Kill()
	_ = c.Wait()
	if _, err := s.Step(); err != nil {
		t.Fatal(err)
	}
	if s.HasReservation("victim") {
		t.Fatal("dead holder's reservation was not reaped")
	}
	if ev := s.Events(); !strings.Contains(ev, `"event":"reap"`) || !strings.Contains(ev, "is gone") {
		t.Fatalf("no structured reap line:\n%s", ev)
	}
}

// A reservation whose job never wrote a pid is reaped after the grace period, not before.
func TestNeverStartedReservationIsReapedAfterGrace(t *testing.T) {
	s, _ := newStore(t, 10, 16384, nil, 0)
	now := time.Unix(1_800_000_000, 0)
	s.Now = func() time.Time { return now }
	if r, _ := s.Admit(AdmitRequest{ID: "ghost", Class: "light", Queue: true}); !r.Admitted {
		t.Fatalf("not admitted: %+v", r)
	}
	now = now.Add(StartGrace - time.Second)
	_, _ = s.Step()
	if !s.HasReservation("ghost") {
		t.Fatal("reaped inside the grace period")
	}
	now = now.Add(2 * time.Second)
	_, _ = s.Step()
	if s.HasReservation("ghost") || !strings.Contains(s.Events(), "never started") {
		t.Fatalf("not reaped after grace:\n%s", s.Events())
	}
}

func TestTokenSerialisesJobsThatNeedIt(t *testing.T) {
	s, _ := newStore(t, 16, 65536, map[string]int{"display": 1}, 0)
	if r, _ := s.Admit(req("b1", "browser", 50)); !r.Admitted {
		t.Fatalf("first browser job not admitted: %+v", r)
	}
	r, _ := s.Admit(req("b2", "browser", 50))
	if !r.Queued || !strings.Contains(r.Reason, "token display: 1/1 held") {
		t.Fatalf("second browser job: %+v; want queued on the token", r)
	}
	if r, _ := s.Admit(req("l1", "light", 60)); !r.Admitted {
		t.Fatalf("a light job needs no token and must be admitted: %+v", r)
	}
}

func TestNeverFitsIsRefusedNotQueued(t *testing.T) {
	s, _ := newStore(t, 12, 65536, map[string]int{}, 0) // no display
	r, _ := s.Admit(req("b", "browser", 50))
	if r.Admitted || r.Queued || !strings.Contains(r.Refused, `"display" token`) {
		t.Fatalf("%+v; want refused (it would queue forever)", r)
	}
	s, _ = newStore(t, 4, 65536, nil, 0) // cap 3 cores
	if r, _ := s.Admit(req("x", "build", 50)); r.Queued || !strings.Contains(r.Refused, "budgets at most 3.0") {
		t.Fatalf("%+v; want refused: build is bigger than the host", r)
	}
}

func TestBudgetShrinksUnderNonFleetLoad(t *testing.T) {
	s, _ := newStore(t, 10, 16384, nil, 0)
	b, _ := s.Measure()
	if ok, why := b.Fits(Need{Class: "build", Cores: 4, MemMB: 4096}); !ok {
		t.Fatalf("idle host refuses a build: %s", why)
	}
	s, _ = newStore(t, 10, 16384, nil, 6.5)
	b, _ = s.Measure()
	if b.NonFleetCores != 6.5 || b.BudgetCores != 2.5 {
		t.Fatalf("non-fleet %.1f budget %.1f; want 6.5 and 2.5", b.NonFleetCores, b.BudgetCores)
	}
	r, _ := s.Admit(req("x", "build", 50))
	if !r.Queued || !strings.Contains(r.Reason, "non-fleet 6.5") {
		t.Fatalf("%+v; want queued, naming the non-fleet load", r)
	}
}

func TestReservationTreeIsNotCountedAsNonFleet(t *testing.T) {
	s, smp := newStore(t, 10, 16384, nil, 0)
	smp.Table = map[int]Proc{
		1:   {PID: 1},
		500: {PID: 500, PPID: 1, Cores: 0.5, MemMB: 200},
		501: {PID: 501, PPID: 500, Cores: 3, MemMB: 2000}, // cargo under the job
		600: {PID: 600, PPID: 1, Cores: 1, MemMB: 500},    // somebody else
	}
	smp.Busy = -1
	_ = s.putReservation(Reservation{ID: "j", Class: "build", Cores: 4, MemMB: 4096, PID: 500, Started: time.Now().Unix()})
	b, err := s.Measure()
	if err != nil {
		t.Fatal(err)
	}
	if b.NonFleetCores != 1 || b.Reservations[0].NowCores != 3.5 {
		t.Fatalf("non-fleet %.1f job %.1f; want 1.0 and 3.5", b.NonFleetCores, b.Reservations[0].NowCores)
	}
}

// Short-lived build processes escape a per-process sample; the machine's busy cores do not.
func TestCPUBudgetFollowsMachineBusyCores(t *testing.T) {
	s, smp := newStore(t, 12, 65536, nil, 0)
	smp.Table = map[int]Proc{1: {PID: 1}, 500: {PID: 500, PPID: 1, Cores: 2}, 600: {PID: 600, PPID: 1, Cores: 3}}
	smp.Busy = 11.4
	_ = s.putReservation(Reservation{ID: "j", Class: "build", Cores: 4, MemMB: 4096, PID: 500, Started: time.Now().Unix()})
	b, err := s.Measure()
	if err != nil {
		t.Fatal(err)
	}
	if b.NonFleetCores != 9.4 {
		t.Fatalf("non-fleet %.1f; want busy 11.4 - job tree 2.0 = 9.4", b.NonFleetCores)
	}
}

// Wired or compressed memory is invisible to per-process RSS; the kernel's figure sees it.
func TestMemoryBudgetFollowsKernelAvailable(t *testing.T) {
	s, smp := newStore(t, 10, 16384, nil, 0)
	smp.MemAvailMB = 3000
	b, err := s.Measure()
	if err != nil {
		t.Fatal(err)
	}
	if b.NonFleetMemMB != 13384 {
		t.Fatalf("non-fleet mem %d; want total-avail 13384", b.NonFleetMemMB)
	}
	r, _ := s.Admit(req("x", "build", 50))
	if !r.Queued || !strings.Contains(r.Reason, "mem: need 4096 MB") {
		t.Fatalf("%+v; want queued on memory", r)
	}
}

// Declared 4 cores, used 6.2: after the raise window the reservation grows to what it used, and
// the class default on this host learns from finished jobs.
func TestOverReservationIsRaisedLoggedAndLearned(t *testing.T) {
	s, smp := newStore(t, 10, 32768, nil, 0)
	c := exec.Command("sleep", "30")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Process.Kill(); _ = c.Wait() }()
	pid := c.Process.Pid
	smp.Table = map[int]Proc{1: {PID: 1}, pid: {PID: pid, PPID: 1, Cores: 6.2, MemMB: 7000}}
	smp.Busy = -1
	_ = s.putReservation(Reservation{ID: "big", Class: "build", Cores: 4, MemMB: 4096, PID: pid,
		Started: time.Now().Add(-200 * time.Second).Unix()})
	if _, err := s.Step(); err != nil {
		t.Fatal(err)
	}
	var r Reservation
	raw, _ := os.ReadFile(filepath.Join(s.Dir, "reservations", "big.json"))
	_ = json.Unmarshal(raw, &r)
	if r.Cores != 7 || r.MemMB != 7168 || !r.Raised {
		t.Fatalf("reservation %+v; want raised to 7 cores / 7168 MB", r)
	}
	if !strings.Contains(s.Events(), `"event":"over_reservation"`) {
		t.Fatal("no over_reservation line")
	}
	for i := 0; i < 3; i++ {
		s.RecordPeak(Reservation{Class: "build", PeakCores: 6.2, PeakMemMB: 7000})
	}
	smp.Table = nil
	b, _ := s.Measure()
	if c := b.Classes["build"]; c.Cores != 7 || c.MemMB != 7168 {
		t.Fatalf("build default %+v; want it to follow the measured peaks (7 / 7168)", c)
	}
}

// Inside the raise window a growing job is measured but not yet raised.
func TestNoRaiseInsideTheWindow(t *testing.T) {
	real := pidAlive
	pidAlive = func(int) bool { return true }
	t.Cleanup(func() { pidAlive = real })
	s, smp := newStore(t, 10, 32768, nil, 0)
	smp.Table = map[int]Proc{1: {PID: 1}, 7: {PID: 7, PPID: 1, Cores: 6}}
	smp.Busy = -1
	_ = s.putReservation(Reservation{ID: "new", Class: "build", Cores: 4, MemMB: 4096, PID: 7, Started: time.Now().Unix()})
	_, _ = s.Step()
	rs, _ := s.Reservations()
	if rs[0].Cores != 4 || rs[0].PeakCores != 6 {
		t.Fatalf("%+v; want peak 6 recorded and reservation still 4", rs[0])
	}
}

func TestPriorityThenFifo(t *testing.T) {
	s, _ := newStore(t, 6, 16384, nil, 0)
	_, _ = s.Admit(req("hold", "build", 50))
	_, _ = s.Admit(req("q1", "build", 50))
	_, _ = s.Admit(req("q2", "build", 50))
	_, _ = s.Admit(req("urgent", "build", 90))
	q, _ := s.Queue()
	var ids []string
	for _, e := range q {
		ids = append(ids, e.ID)
	}
	if strings.Join(ids, ",") != "urgent,q1,q2" {
		t.Fatalf("queue order %v; want urgent,q1,q2", ids)
	}
}

// A big job at the head of the queue blocks the small job behind it, even though the small one
// would fit. That is the cost of never starving the head.
func TestHeadBlocksSmallerJobsBehindIt(t *testing.T) {
	s, _ := newStore(t, 6, 16384, nil, 0)  // cap 5 cores
	_, _ = s.Admit(req("r1", "build", 50)) // 4 of 5 held
	if r, _ := s.Admit(req("head", "build", 50)); !r.Queued {
		t.Fatalf("head should queue: %+v", r)
	}
	r, _ := s.Admit(req("small", "light", 50)) // 1 core is free, but the head is ahead
	if !r.Queued || !strings.Contains(r.Reason, "queued ahead") {
		t.Fatalf("%+v; want the light job held behind the head", r)
	}
	r, _ = s.Admit(req("vip", "light", 60)) // higher priority is not behind anyone
	if !r.Admitted {
		t.Fatalf("%+v; a higher-priority job that fits goes first", r)
	}
}

func TestDequeue(t *testing.T) {
	s, _ := newStore(t, 6, 16384, nil, 0)
	_, _ = s.Admit(req("hold", "build", 50))
	_, _ = s.Admit(req("w", "build", 50))
	if ok, _ := s.Dequeue("w", "cancel"); !ok {
		t.Fatal("dequeue of a queued job failed")
	}
	if ok, _ := s.Dequeue("w", "again"); ok {
		t.Fatal("a second dequeue must report not-queued")
	}
}

func TestBudgetSnapshotIsWrittenByStep(t *testing.T) {
	s, _ := newStore(t, 10, 16384, nil, 0)
	_, _ = s.Admit(req("a", "light", 50))
	if _, err := s.Step(); err != nil {
		t.Fatal(err)
	}
	var b Budget
	raw, err := os.ReadFile(filepath.Join(s.Dir, "budget.json"))
	if err != nil || json.Unmarshal(raw, &b) != nil || len(b.Reservations) != 1 || b.Reservations[0].ID != "a" {
		t.Fatalf("snapshot %s (%v)", raw, err)
	}
}

func TestEffectiveClassesNeedThreeSamplesAndRespectBounds(t *testing.T) {
	base := DefaultClasses()
	h := History{"build": {{Cores: 9, MemMB: 9000}, {Cores: 9, MemMB: 9000}}}
	if c := EffectiveClasses(base, h, 20, 99999)["build"]; c.Cores != 4 {
		t.Fatalf("two samples must not move the default: %+v", c)
	}
	h["build"] = append(h["build"], PeakRow{Cores: 9, MemMB: 9000})
	if c := EffectiveClasses(base, h, 20, 99999)["build"]; c.Cores != 9 || c.MemMB != 9216 {
		t.Fatalf("three samples: %+v; want 9 / 9216", c)
	}
	if c := EffectiveClasses(base, h, 5, 6000)["build"]; c.Cores != 5 || c.MemMB != 6000 {
		t.Fatalf("capped by the host: %+v; want 5 / 6000", c)
	}
	h["build"] = []PeakRow{{Cores: 1, MemMB: 100}, {Cores: 1, MemMB: 100}, {Cores: 1, MemMB: 100}}
	if c := EffectiveClasses(base, h, 20, 99999)["build"]; c.Cores != 4 || c.MemMB != 4096 {
		t.Fatalf("never below the base: %+v", c)
	}
}

func TestPlacePrefersAHostThatFitsThenTheShortestQueue(t *testing.T) {
	cl := DefaultClasses()
	mk := func(name string, ncpu int, nonFleet float64) *Budget {
		return NewBudget(name, Capacity{NCPU: ncpu, MemTotalMB: 32768, MemAvailMB: -1}, Proc{Cores: nonFleet}, cl)
	}
	a, b, c := mk("a", 10, 6), mk("b", 12, 1), mk("c", 6, 0)
	n := Need{Class: "build", Cores: 4, MemMB: 4096}
	if got := Place([]*Budget{a, b, c}, n); got != 1 {
		t.Fatalf("placed on %d; want b (most free cores that fits)", got)
	}
	b.Take(Reservation{ID: "x", Cores: 8, MemMB: 8192})
	c.Take(Reservation{ID: "y", Cores: 4, MemMB: 4096})
	a.Queue = []QueueEntry{{Cores: 4}, {Cores: 4}}
	b.Queue = []QueueEntry{{Cores: 4}}
	if got := Place([]*Budget{a, b, c}, n); got != 2 {
		t.Fatalf("placed on %d; want c (nothing fits, c has the shortest queue)", got)
	}
	if got := Place([]*Budget{mk("tiny", 2, 0)}, n); got != -1 {
		t.Fatalf("placed on %d; want -1 (never fits)", got)
	}
}

// The treeUsage ownership rule: a process is counted once, in the first tree that reaches it.
func TestTreeUsageCountsEachProcessOnce(t *testing.T) {
	procs := map[int]Proc{
		1: {PID: 1}, 10: {PID: 10, PPID: 1, Cores: 1}, 11: {PID: 11, PPID: 10, Cores: 2}, 99: {PID: 99, PPID: 1, Cores: 5},
	}
	per, rest := treeUsage(procs, map[string]int{"j": 10})
	if per["j"].Cores != 3 || rest.Cores != 5 {
		t.Fatalf("job %.1f rest %.1f; want 3 and 5", per["j"].Cores, rest.Cores)
	}
}
