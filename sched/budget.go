package sched

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

func round1(f float64) float64 { return math.Round(f*10) / 10 }

func roundMem(mb int64) int64 { return (mb + 255) / 256 * 256 }

func pct(v []float64, p float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[int(math.Ceil(p*float64(len(s))))-1]
}

// NewBudget builds a budget from raw capacity, the load that is not ours, and the classes.
// It is pure: the store and the simulator both use it.
func NewBudget(host string, c Capacity, nonFleet Proc, classes map[string]Class) *Budget {
	memRes := int64(math.Max(1024, float64(c.MemTotalMB)*OSReserveMemPct))
	b := &Budget{
		Host: host, NCPU: c.NCPU, MemTotalMB: c.MemTotalMB,
		CapCores: float64(c.NCPU) - OSReserveCores, CapMemMB: c.MemTotalMB - memRes,
		NonFleetCores: round1(nonFleet.Cores), NonFleetMemMB: int64(nonFleet.MemMB),
		Tokens: c.Tokens, TokensHeld: map[string]int{}, Classes: classes,
	}
	if b.Tokens == nil {
		b.Tokens = map[string]int{}
	}
	b.BudgetCores = round1(b.CapCores - b.NonFleetCores)
	b.BudgetMemMB = b.CapMemMB - b.NonFleetMemMB
	b.recompute()
	return b
}

func (b *Budget) recompute() {
	b.FreeCores = round1(b.BudgetCores - b.ReservedCores)
	b.FreeMemMB = b.BudgetMemMB - b.ReservedMemMB
	b.OverBudget = b.FreeCores < 0 || b.FreeMemMB < 0
}

// NeedFor resolves a class (and optional overrides) against this host's defaults.
func (b *Budget) NeedFor(class string, cores float64, mem int64) (Need, error) {
	c, ok := b.Classes[class]
	if !ok {
		return Need{}, fmt.Errorf("unknown class %q", class)
	}
	n := Need{Class: class, Cores: c.Cores, MemMB: c.MemMB, Tokens: c.Tokens}
	if cores > 0 {
		n.Cores = cores
	}
	if mem > 0 {
		n.MemMB = mem
	}
	return n, nil
}

// NeverFits says why a need can never be admitted on this host, or "". Queueing it would wait forever.
func (b *Budget) NeverFits(n Need) string {
	if n.Cores > b.CapCores {
		return fmt.Sprintf("%s needs %.1f cores; this host budgets at most %.1f", n.Class, n.Cores, b.CapCores)
	}
	if n.MemMB > b.CapMemMB {
		return fmt.Sprintf("%s needs %d MB; this host budgets at most %d MB", n.Class, n.MemMB, b.CapMemMB)
	}
	for _, t := range n.Tokens {
		if b.Tokens[t] == 0 {
			return fmt.Sprintf("%s needs the %q token; this host has none", n.Class, t)
		}
	}
	return ""
}

// Fits says whether n fits now, and if not, what is short.
func (b *Budget) Fits(n Need) (bool, string) {
	var short []string
	if n.Cores > b.FreeCores+1e-9 {
		short = append(short, fmt.Sprintf("cores: need %.1f, free %.1f (cap %.1f - non-fleet %.1f - reserved %.1f)",
			n.Cores, b.FreeCores, b.CapCores, b.NonFleetCores, b.ReservedCores))
	}
	if n.MemMB > b.FreeMemMB {
		short = append(short, fmt.Sprintf("mem: need %d MB, free %d MB (cap %d - non-fleet %d - reserved %d)",
			n.MemMB, b.FreeMemMB, b.CapMemMB, b.NonFleetMemMB, b.ReservedMemMB))
	}
	for _, t := range n.Tokens {
		if b.TokensHeld[t] >= b.Tokens[t] {
			short = append(short, fmt.Sprintf("token %s: %d/%d held", t, b.TokensHeld[t], b.Tokens[t]))
		}
	}
	return len(short) == 0, strings.Join(short, "; ")
}

// Take books a reservation against the budget.
func (b *Budget) Take(r Reservation) {
	b.ReservedCores += r.Cores
	b.ReservedMemMB += r.MemMB
	for _, t := range r.Tokens {
		b.TokensHeld[t]++
	}
	b.Reservations = append(b.Reservations, r)
	b.recompute()
}

// Give returns a reservation's hold.
func (b *Budget) Give(r Reservation) {
	b.ReservedCores -= r.Cores
	b.ReservedMemMB -= r.MemMB
	for _, t := range r.Tokens {
		b.TokensHeld[t]--
	}
	for i := range b.Reservations {
		if b.Reservations[i].ID == r.ID {
			b.Reservations = append(b.Reservations[:i], b.Reservations[i+1:]...)
			break
		}
	}
	b.recompute()
}

// History is the per-class record of measured peaks on one host.
type History map[string][]PeakRow

// PeakRow is one finished job's measured peak.
type PeakRow struct {
	Cores float64 `json:"cores"`
	MemMB int64   `json:"mem_mb"`
	At    int64   `json:"at"`
}

// EffectiveClasses are the footprints admissions use: the base, raised to the 80th percentile of
// this host's measured peaks for that class once there are at least 3. Never below the base,
// never above the host's cap.
func EffectiveClasses(base map[string]Class, h History, capCores float64, capMem int64) map[string]Class {
	out := map[string]Class{}
	for name, c := range base {
		if rows := h[name]; len(rows) >= 3 {
			cs := make([]float64, len(rows))
			ms := make([]float64, len(rows))
			for i, r := range rows {
				cs[i], ms[i] = r.Cores, float64(r.MemMB)
			}
			c.Cores = math.Max(c.Cores, math.Min(math.Ceil(pct(cs, 0.8)), capCores))
			c.MemMB = max(c.MemMB, min(roundMem(int64(pct(ms, 0.8))), capMem))
		}
		out[name] = c
	}
	return out
}
