package sched

// Place picks a host for a need from each host's current budget. A host that fits now wins, the
// one with the most free cores first. When none fits, the job goes to the host with the least
// queued work per core. It returns -1 when no host can ever take the need.
func Place(budgets []*Budget, n Need) int {
	best, bestFree := -1, -1.0
	for i, b := range budgets {
		if b.NeverFits(n) != "" {
			continue
		}
		if ok, _ := b.Fits(n); ok && len(b.Queue) == 0 && b.FreeCores > bestFree {
			best, bestFree = i, b.FreeCores
		}
	}
	if best >= 0 {
		return best
	}
	least := -1.0
	for i, b := range budgets {
		if b.NeverFits(n) != "" {
			continue
		}
		var q float64
		for _, e := range b.Queue {
			q += e.Cores
		}
		q /= b.CapCores
		if least < 0 || q < least {
			best, least = i, q
		}
	}
	return best
}
