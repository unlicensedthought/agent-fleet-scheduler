// fleetctl is the host-side command line for the reservation store.
//
//	fleetctl admit   --id ID [--class C] [--priority P] [--queue]
//	fleetctl release ID
//	fleetctl sched   [--loop --every S]
//	fleetctl budget  [--json]
//	fleetctl reserve [--class C] [--priority P] [--wait S] -- CMD...
//
// State lives in $FLEET_HOST_STATE (default ~/.local/state/agent-fleet-scheduler). Host capacity
// is read from /proc; FLEET_TEST_HOST="<ncpu>,<memMB>" pins a fixed idle machine instead.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/unlicensedthought/agent-fleet-scheduler/sched"
)

func open() *sched.Store {
	dir := os.Getenv("FLEET_HOST_STATE")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".local", "state", "agent-fleet-scheduler")
	}
	host := os.Getenv("FLEET_HOST")
	if host == "" {
		host, _ = os.Hostname()
	}
	var smp sched.Sampler = sched.Linux{}
	if v := os.Getenv("FLEET_TEST_HOST"); v != "" {
		a, b, _ := strings.Cut(v, ",")
		n, _ := strconv.Atoi(a)
		m, _ := strconv.ParseInt(b, 10, 64)
		smp = sched.Static{NCPU: n, MemTotalMB: m, Tokens: map[string]int{}}
	}
	st, err := sched.Open(dir, host, smp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleetctl: state %s: %v\n", dir, err)
		os.Exit(1)
	}
	return st
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: fleetctl admit|release|sched|budget|reserve ...")
		os.Exit(2)
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "admit":
		os.Exit(admit(args))
	case "release":
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "usage: fleetctl release ID")
			os.Exit(2)
		}
		ok, err := open().Release(args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, "fleetctl release:", err)
			os.Exit(1)
		}
		if !ok {
			fmt.Fprintf(os.Stderr, "fleetctl release: %s held no reservation\n", args[0])
			os.Exit(4)
		}
	case "sched":
		os.Exit(schedCmd(args))
	case "budget":
		fs := flag.NewFlagSet("budget", flag.ExitOnError)
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args)
		b, err := open().Measure()
		if err != nil {
			fmt.Fprintln(os.Stderr, "fleetctl budget:", err)
			os.Exit(1)
		}
		if *asJSON {
			j, _ := json.Marshal(b)
			fmt.Println(string(j))
			return
		}
		fmt.Printf("free %.1f cores / %d MB of cap %.1f / %d MB (non-fleet %.1f cores; reserved %.1f / %d MB; %d queued)\n",
			b.FreeCores, b.FreeMemMB, b.CapCores, b.CapMemMB, b.NonFleetCores, b.ReservedCores, b.ReservedMemMB, len(b.Queue))
	case "reserve":
		os.Exit(reserve(args))
	default:
		fmt.Fprintf(os.Stderr, "fleetctl: unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

func admit(args []string) int {
	fs := flag.NewFlagSet("admit", flag.ContinueOnError)
	var req sched.AdmitRequest
	fs.StringVar(&req.ID, "id", "", "job id")
	fs.StringVar(&req.Class, "class", "build", "light|build|browser")
	fs.IntVar(&req.PID, "pid", 0, "holder pid")
	fs.IntVar(&req.Priority, "priority", 50, "higher runs first")
	fs.BoolVar(&req.Queue, "queue", false, "enqueue when it does not fit")
	if err := fs.Parse(args); err != nil || req.ID == "" {
		fmt.Fprintln(os.Stderr, "usage: fleetctl admit --id ID [--class C] [--pid N] [--priority P] [--queue]")
		return 2
	}
	r, err := open().Admit(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fleetctl admit:", err)
		return 1
	}
	j, _ := json.Marshal(r)
	fmt.Println(string(j))
	return 0
}

func schedCmd(args []string) int {
	fs := flag.NewFlagSet("sched", flag.ContinueOnError)
	loop := fs.Bool("loop", false, "keep scheduling")
	every := fs.Int("every", 15, "seconds between passes with --loop")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	st := open()
	for {
		granted, err := st.Step()
		if err != nil {
			fmt.Fprintln(os.Stderr, "fleetctl sched:", err)
			if !*loop {
				return 1
			}
		}
		for _, r := range granted {
			fmt.Printf("granted %s\n", r.ID)
		}
		if !*loop {
			return 0
		}
		time.Sleep(time.Duration(*every) * time.Second)
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return 1
}

// reserve runs CMD under a reservation on this host, waiting in the queue when it does not fit.
func reserve(args []string) int {
	fs := flag.NewFlagSet("reserve", flag.ContinueOnError)
	id := fs.String("id", "", "reservation id")
	class := fs.String("class", "build", "light|build|browser")
	prio := fs.Int("priority", 50, "higher runs first")
	wait := fs.Int("wait", 3600, "give up after this many seconds in the queue (0 = no limit)")
	if err := fs.Parse(args); err != nil || fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: fleetctl reserve [--class C] [--priority P] [--wait S] -- CMD...")
		return 2
	}
	if *id == "" {
		var b [4]byte
		_, _ = rand.Read(b[:])
		*id = "hold-" + hex.EncodeToString(b[:])
	}
	st := open()
	r, err := st.Admit(sched.AdmitRequest{ID: *id, Class: *class, PID: os.Getpid(), Priority: *prio, Queue: true, Waiter: os.Getpid()})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fleetctl reserve:", err)
		return 1
	}
	if r.Refused != "" {
		fmt.Fprintf(os.Stderr, "fleetctl reserve: refused on %s: %s\n", r.Host, r.Refused)
		return 3
	}
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	if r.Queued {
		fmt.Fprintf(os.Stderr, "[sched] %s queued: position %d of %d (%s); waiting\n", *id, r.Position, r.Depth, r.Reason)
		t0 := time.Now()
		for !st.HasReservation(*id) {
			select {
			case sig := <-sigs:
				_, _ = st.Dequeue(*id, "waiter got "+sig.String())
				return 128 + int(sig.(syscall.Signal))
			case <-time.After(2 * time.Second):
			}
			if _, err := st.Step(); err != nil {
				fmt.Fprintln(os.Stderr, "[sched] pass:", err)
			}
			if *wait > 0 && time.Since(t0) > time.Duration(*wait)*time.Second && !st.HasReservation(*id) {
				_, _ = st.Dequeue(*id, "wait timeout")
				fmt.Fprintf(os.Stderr, "[sched] %s: gave up after %ds in the queue\n", *id, *wait)
				return 124
			}
		}
		fmt.Fprintf(os.Stderr, "[sched] %s admitted after %ds\n", *id, int(time.Since(t0).Seconds()))
	}
	defer func() {
		if _, err := st.Release(*id); err != nil {
			fmt.Fprintln(os.Stderr, "[sched] release:", err)
		}
		if _, err := st.Step(); err != nil {
			fmt.Fprintln(os.Stderr, "[sched] pass after release:", err)
		}
	}()
	argv := fs.Args()
	c := exec.Command(argv[0], argv[1:]...)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := c.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "fleetctl reserve:", err)
		return 127
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	for {
		select {
		case sig := <-sigs:
			_ = c.Process.Signal(sig)
		case err := <-done:
			return exitCode(err)
		}
	}
}
