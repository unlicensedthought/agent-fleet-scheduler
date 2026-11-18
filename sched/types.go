// Package sched admits jobs onto a host by reserving their peak resource use, not by looking at
// the load right now.
//
// A job asks for a class (light, build, browser). The scheduler holds that class's peak cores and
// memory for the job's whole life, under a host-wide file lock, and admits a job only when every
// reservation plus the new one fits the host's budget. What doesn't fit waits in a durable queue.
package sched

import "time"

// Class is a job's declared peak footprint.
type Class struct {
	Name   string   `json:"name"`
	Cores  float64  `json:"cores"`
	MemMB  int64    `json:"mem_mb"`
	Tokens []string `json:"tokens,omitempty"`
}

// DefaultClasses are the base footprints. Measured peaks can raise them per host (see History).
func DefaultClasses() map[string]Class {
	return map[string]Class{
		"light":   {Name: "light", Cores: 1, MemMB: 1024},
		"build":   {Name: "build", Cores: 4, MemMB: 4096},
		"browser": {Name: "browser", Cores: 2, MemMB: 2048, Tokens: []string{"display"}},
	}
}

const (
	OSReserveCores  = 1.0  // one core is never budgeted
	OSReserveMemPct = 0.10 // nor 10% of RAM (at least 1 GB)
	RaiseAfter      = 180 * time.Second
	StartGrace      = 180 * time.Second // a reservation with no holder pid after this never started
	HistoryKeep     = 20
	QueueWarnEvery  = 300 * time.Second // a head that cannot start says why this often
)

// Reservation is one admitted job's hold on the host.
type Reservation struct {
	ID        string   `json:"id"`
	Class     string   `json:"class"`
	Cores     float64  `json:"cores"`
	MemMB     int64    `json:"mem_mb"`
	Tokens    []string `json:"tokens,omitempty"`
	PID       int      `json:"pid,omitempty"` // root of the job's process tree; 0 until it starts
	Started   int64    `json:"started_unix"`
	PeakCores float64  `json:"peak_cores"`
	PeakMemMB int64    `json:"peak_mem_mb"`
	Raised    bool     `json:"raised,omitempty"`
	NowCores  float64  `json:"now_cores"`
	NowMemMB  int64    `json:"now_mem_mb"`
}

// QueueEntry is one job waiting for the budget.
type QueueEntry struct {
	ID       string   `json:"id"`
	Class    string   `json:"class"`
	Priority int      `json:"priority"`
	Seq      int64    `json:"seq"`
	Cores    float64  `json:"cores"`
	MemMB    int64    `json:"mem_mb"`
	Tokens   []string `json:"tokens,omitempty"`
	PID      int      `json:"pid,omitempty"` // the waiting process, when it is alive and polling
	LastWarn int64    `json:"last_warn,omitempty"`
	Position int      `json:"position"`
}

// Need is what one admission asks for.
type Need struct {
	Class  string
	Cores  float64
	MemMB  int64
	Tokens []string
}

// Budget is a host's admission picture at one instant.
type Budget struct {
	Host          string           `json:"host"`
	NCPU          int              `json:"ncpu"`
	MemTotalMB    int64            `json:"mem_total_mb"`
	CapCores      float64          `json:"cap_cores"`
	CapMemMB      int64            `json:"cap_mem_mb"`
	NonFleetCores float64          `json:"non_fleet_cores"`
	NonFleetMemMB int64            `json:"non_fleet_mem_mb"`
	BudgetCores   float64          `json:"budget_cores"`
	BudgetMemMB   int64            `json:"budget_mem_mb"`
	ReservedCores float64          `json:"reserved_cores"`
	ReservedMemMB int64            `json:"reserved_mem_mb"`
	FreeCores     float64          `json:"free_cores"`
	FreeMemMB     int64            `json:"free_mem_mb"`
	Tokens        map[string]int   `json:"tokens"`
	TokensHeld    map[string]int   `json:"tokens_held"`
	Classes       map[string]Class `json:"classes"`
	Reservations  []Reservation    `json:"reservations"`
	Queue         []QueueEntry     `json:"queue"`
	OverBudget    bool             `json:"over_budget"`

	usage map[string]Proc // per reservation, from this sample
}

// AdmitRequest is one admission call.
type AdmitRequest struct {
	ID       string
	Class    string
	Cores    float64 // optional override of the class default
	MemMB    int64   // optional override of the class default
	PID      int     // holder process; 0 when the job starts after admission
	Priority int     // higher runs first
	Queue    bool    // enqueue when it does not fit (else just report)
	Waiter   int     // pid of a live process polling for its turn; the queue drops it when that pid dies
}

// AdmitResult is what Admit returns.
type AdmitResult struct {
	ID       string  `json:"id"`
	Admitted bool    `json:"admitted"`
	Queued   bool    `json:"queued"`
	Position int     `json:"position,omitempty"`
	Depth    int     `json:"depth,omitempty"`
	Refused  string  `json:"refused,omitempty"`
	Reason   string  `json:"reason,omitempty"`
	Cores    float64 `json:"cores"`
	MemMB    int64   `json:"mem_mb"`
	Host     string  `json:"host"`
}

func timeUnix(sec int64) time.Time { return time.Unix(sec, 0) }
