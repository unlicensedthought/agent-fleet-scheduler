// fleetsim replays a synthetic job stream under instantaneous-load admission and under
// reservation admission, and prints both side by side.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/unlicensedthought/agent-fleet-scheduler/sim"
)

func main() {
	file := flag.String("scenario", "examples/incident.json", "scenario JSON")
	maxS := flag.Int("max", 20000, "stop after this many simulated seconds")
	asJSON := flag.Bool("json", false, "print JSON")
	flag.Parse()
	raw, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fleetsim:", err)
		os.Exit(1)
	}
	var sc sim.Scenario
	if err := json.Unmarshal(raw, &sc); err != nil {
		fmt.Fprintln(os.Stderr, "fleetsim:", file, err)
		os.Exit(1)
	}
	var out []sim.Result
	for _, p := range []string{sim.Load, sim.Reserve} {
		r, err := sim.Simulate(sc, p, *maxS)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fleetsim:", err)
			os.Exit(1)
		}
		out = append(out, r)
	}
	if *asJSON {
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
		return
	}
	fmt.Printf("SIMULATION: synthetic hosts and jobs, %d jobs; nothing here was measured on a real machine\n", len(sc.Jobs))
	for _, r := range out {
		fmt.Printf("\npolicy %-8s makespan %5ds  mean wait %6.1fs  most jobs at once %d  unfinished %d\n",
			r.Policy, r.Makespan, r.MeanWaitS, r.MaxAtOnce, r.Unfinished)
		for _, h := range r.Hosts {
			fmt.Printf("  host %-3s %2d cores  peak demand %5.1f  seconds over capacity %5d\n", h.Name, h.NCPU, h.PeakDemand, h.OverSeconds)
		}
	}
}
