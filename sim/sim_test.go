package sim

import (
	"encoding/json"
	"os"
	"testing"
)

func load(t *testing.T, path string) Scenario {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var sc Scenario
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatal(err)
	}
	return sc
}

// The incident shape: four builds in 35 s onto one host. The load policy lets all four in because
// each one's load is tiny at the moment it asks. Reservation lets one in at a time.
func TestIncidentLoadPolicyOverloadsReservationDoesNot(t *testing.T) {
	sc := load(t, "../examples/incident.json")
	l, err := Simulate(sc, Load, 20000)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Simulate(sc, Reserve, 20000)
	if err != nil {
		t.Fatal(err)
	}
	if l.MaxAtOnce != 4 || l.Hosts[0].PeakDemand <= float64(l.Hosts[0].NCPU)*2 {
		t.Fatalf("load policy %+v; want all four admitted and demand over 2x the cores (the known-bad case)", l)
	}
	if r.MaxAtOnce != 1 || r.Hosts[0].OverSeconds != 0 || r.Hosts[0].PeakDemand > float64(r.Hosts[0].NCPU) {
		t.Fatalf("reserve policy %+v; want one job at a time and never over capacity", r)
	}
	if l.Unfinished != 0 || r.Unfinished != 0 {
		t.Fatalf("unfinished: load %d reserve %d", l.Unfinished, r.Unfinished)
	}
	if r.Makespan <= l.Makespan {
		t.Fatalf("reserve makespan %d <= load makespan %d; reservation is meant to cost wall time here", r.Makespan, l.Makespan)
	}
}

func TestThreeHostsReserveStaysWithinCapacity(t *testing.T) {
	sc := load(t, "../examples/three-hosts.json")
	r, err := Simulate(sc, Reserve, 20000)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range r.Hosts {
		if h.OverSeconds != 0 {
			t.Fatalf("host %s over capacity for %ds under reservation", h.Name, h.OverSeconds)
		}
	}
	l, _ := Simulate(sc, Load, 20000)
	over := 0
	for _, h := range l.Hosts {
		over += h.OverSeconds
	}
	if over == 0 {
		t.Fatal("load policy never overloaded; the scenario no longer shows the failure")
	}
}

func TestUnknownClassAndPolicyAreErrors(t *testing.T) {
	sc := load(t, "../examples/incident.json")
	if _, err := Simulate(sc, "nope", 10); err == nil {
		t.Fatal("unknown policy must error")
	}
	sc.Jobs[0].Class = "huge"
	if _, err := Simulate(sc, Reserve, 10); err == nil {
		t.Fatal("unknown class must error")
	}
}
