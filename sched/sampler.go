package sched

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Proc is one process's footprint.
type Proc struct {
	PID   int
	PPID  int
	Cores float64 // CPU in cores (1.0 = one core busy)
	MemMB float64 // resident set
}

// Capacity is the raw machine.
type Capacity struct {
	NCPU       int
	MemTotalMB int64
	MemAvailMB int64 // what the kernel can hand out now; -1 when unknown
	Tokens     map[string]int
}

// Sampler measures the host. The store calls it on every admission.
type Sampler interface {
	Capacity() (Capacity, error)
	// Procs returns every process and the machine's busy cores over the same interval
	// (-1 when the machine figure is unavailable).
	Procs() (map[int]Proc, float64, error)
}

// Static is a fixed host: tests and the simulator use it. NonFleetCores is load that belongs to
// no reservation.
type Static struct {
	NCPU          int
	MemTotalMB    int64
	MemAvailMB    int64
	Tokens        map[string]int
	NonFleetCores float64
	Table         map[int]Proc // when set, returned as the process table instead of a synthetic one
	Busy          float64      // machine busy cores with Table; -1 for per-process sums
}

func (s Static) Capacity() (Capacity, error) {
	avail := s.MemAvailMB
	if avail == 0 {
		avail = -1
	}
	return Capacity{NCPU: s.NCPU, MemTotalMB: s.MemTotalMB, MemAvailMB: avail, Tokens: s.Tokens}, nil
}

func (s Static) Procs() (map[int]Proc, float64, error) {
	if s.Table != nil {
		return s.Table, s.Busy, nil
	}
	return map[int]Proc{1: {PID: 1}, 4242: {PID: 4242, PPID: 1, Cores: s.NonFleetCores, MemMB: 300}}, -1, nil
}

// Linux reads /proc. Non-fleet load is the machine's busy cores over one second (from
// /proc/stat) minus the reservation trees: a per-process sum misses the short-lived processes a
// build spawns by the thousand.
type Linux struct{}

func (Linux) Capacity() (Capacity, error) {
	c := Capacity{NCPU: runtime.NumCPU(), MemAvailMB: -1, Tokens: map[string]int{}}
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return c, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) < 2 {
			continue
		}
		kb, _ := strconv.ParseInt(fs[1], 10, 64)
		switch fs[0] {
		case "MemTotal:":
			c.MemTotalMB = kb / 1024
		case "MemAvailable:":
			c.MemAvailMB = kb / 1024
		}
	}
	if c.MemTotalMB == 0 {
		return c, fmt.Errorf("/proc/meminfo has no MemTotal")
	}
	return c, nil
}

func cpuTicks() (busy, total int64, err error) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	line, _, _ := strings.Cut(string(b), "\n")
	fs := strings.Fields(line)
	if len(fs) < 6 || fs[0] != "cpu" {
		return 0, 0, fmt.Errorf("unexpected /proc/stat: %q", line)
	}
	for i, v := range fs[1:] {
		n, _ := strconv.ParseInt(v, 10, 64)
		total += n
		if i != 3 && i != 4 { // idle, iowait
			busy += n
		}
	}
	return busy, total, nil
}

type statRow struct {
	ppid  int
	ticks int64
	rss   int64
}

func readStats() (map[int]statRow, error) {
	dirs, err := filepath.Glob("/proc/[0-9]*/stat")
	if err != nil {
		return nil, err
	}
	m := map[int]statRow{}
	for _, p := range dirs {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue // exited
		}
		s := string(raw)
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		fs := strings.Fields(s[i+2:])
		if len(fs) < 22 {
			continue
		}
		pid, _ := strconv.Atoi(strings.Fields(s)[0])
		ppid, _ := strconv.Atoi(fs[1])
		ut, _ := strconv.ParseInt(fs[11], 10, 64)
		st, _ := strconv.ParseInt(fs[12], 10, 64)
		rss, _ := strconv.ParseInt(fs[21], 10, 64)
		m[pid] = statRow{ppid, ut + st, rss}
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("no readable /proc/*/stat")
	}
	return m, nil
}

func (Linux) Procs() (map[int]Proc, float64, error) {
	const hz = 100.0
	b0, t0, err := cpuTicks()
	if err != nil {
		return nil, -1, err
	}
	s0, err := readStats()
	if err != nil {
		return nil, -1, err
	}
	start := time.Now()
	time.Sleep(time.Second)
	b1, t1, err := cpuTicks()
	if err != nil {
		return nil, -1, err
	}
	s1, err := readStats()
	if err != nil {
		return nil, -1, err
	}
	secs := time.Since(start).Seconds()
	busy := -1.0
	if t1 > t0 {
		busy = float64(b1-b0) / float64(t1-t0) * float64(runtime.NumCPU())
	}
	page := float64(os.Getpagesize())
	out := map[int]Proc{}
	for pid, r := range s1 {
		cores := 0.0
		if prev, ok := s0[pid]; ok {
			cores = float64(r.ticks-prev.ticks) / hz / secs
		}
		out[pid] = Proc{PID: pid, PPID: r.ppid, Cores: cores, MemMB: float64(r.rss) * page / (1 << 20)}
	}
	return out, busy, nil
}

// treeUsage sums each root's process tree. It returns per-root usage and the remainder: what
// every process outside all trees uses (the non-fleet load).
func treeUsage(procs map[int]Proc, roots map[string]int) (map[string]Proc, Proc) {
	kids := map[int][]int{}
	for pid, p := range procs {
		kids[p.PPID] = append(kids[p.PPID], pid)
	}
	owned := map[int]bool{}
	per := map[string]Proc{}
	for id, root := range roots {
		var u Proc
		if _, ok := procs[root]; !ok || root <= 1 {
			per[id] = u
			continue
		}
		stack := []int{root}
		for len(stack) > 0 {
			pid := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if owned[pid] {
				continue
			}
			owned[pid] = true
			p := procs[pid]
			u.Cores += p.Cores
			u.MemMB += p.MemMB
			stack = append(stack, kids[pid]...)
		}
		per[id] = u
	}
	var rest Proc
	for pid, p := range procs {
		if !owned[pid] {
			rest.Cores += p.Cores
			rest.MemMB += p.MemMB
		}
	}
	return per, rest
}
