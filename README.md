# agent-fleet-scheduler

Admit jobs onto a host by what they will need at their peak, not by how busy the host is right now.

## The problem

A scheduler that looks at current load before it starts a job has a hole in it. A job that's just
starting is quiet. It hasn't run `npm ci`, a compiler and a browser yet. So the load check passes
for the first job, and the second, and the third and fourth, all inside a minute. Then they all
grow at once.

That's what happened to one of my Macs. Four agent jobs were admitted within 35 seconds while the
machine showed a load of 6.2 on 10 cores. Each grew into a dependency install, a build, a dev server
and a browser. The load reached 44, with 32 agent processes and 24 browser crashes after it.

This repo is the part of that scheduler that fixes it: admission by reservation.

## What it does

- Every job declares a class: `light` (1 core, 1 GB), `build` (4 cores, 4 GB) or `browser`
  (2 cores, 2 GB, plus one exclusive `display` token). The class is the job's peak, held for its
  whole life.
- A job is admitted only if every reservation already held, plus its own, fits the host's budget.
- The budget is capacity minus one core, minus 10% of RAM, minus what processes that aren't ours
  use right now. It's recomputed on every admission.
- Every read-decide-write happens under one host-wide `flock`, so two jobs asking in the same
  second can't both pass.
- What doesn't fit goes into a durable queue (priority first, then arrival order). The head of
  the queue blocks the jobs behind it, so a big job can't be starved by a stream of small ones.
- A reservation is dropped when its holder's pid is gone, or when no pid showed up within 180 s.
- If a job grows past its class, the scheduler raises its reservation to what was measured (after
  180 s) and logs `over_reservation`. Measured peaks are kept per class. After 3 samples the
  class default on that host is the 80th percentile, never below the base.
- A job that can never fit (bigger than the host, or needs a token the host doesn't have) is refused,
  not queued forever.
- Every decision is a JSON line in `events.jsonl`.

## Quick start

Needs Go 1.22 or newer and Linux (the measurement reads `/proc`; the library and simulator don't
need it).

```
go build -o fleetctl ./cmd/fleetctl
export FLEET_HOST_STATE=$(mktemp -d)

./fleetctl admit --id job-1 --class build --queue     # {"admitted":true,...}
./fleetctl admit --id job-2 --class build --queue     # fits? depends on your host
./fleetctl budget
./fleetctl sched                                      # promote queued jobs that now fit
./fleetctl release job-1

./fleetctl reserve --class build -- make test         # wait for a slot, run, release
```

`FLEET_TEST_HOST="6,16384"` pins a fixed idle host (6 cores, 16 GB) so you can try it anywhere.

As a library:

```go
st, _ := sched.Open(dir, "a", sched.Linux{})
res, _ := st.Admit(sched.AdmitRequest{ID: "job-1", Class: "build", PID: os.Getpid(), Queue: true})
// res.Admitted, or res.Queued with res.Position and res.Reason ("cores: need 4.0, free 1.0 ...")
granted, _ := st.Step() // run this on a timer; start every job it returns
st.Release("job-1")
```

## See the failure

`fleetsim` replays a made-up job stream under both policies. Everything in it is synthetic.

```
go run ./cmd/fleetsim -scenario examples/incident.json
go run ./cmd/fleetsim -scenario examples/three-hosts.json
```

On the one-host scenario (4 builds that declare 4 cores and really use 8, arriving over 35 s on a
10-core host):

| policy | peak demand | seconds over capacity | makespan |
|---|---|---|---|
| admit while load < 80% of cores | 34.0 cores | 1816 | 1877 s |
| admit by reservation | 10.0 cores | 0 | 2399 s |

Reservation costs wall time here: jobs wait. That's the trade. On the three-host scenario (hosts
`a`, `b`, `c`; 12 jobs) the same pattern holds: no host over capacity under reservation, 56% longer
makespan.

The model is small. A job ramps to its peak, a host slows every job on it when demand exceeds its
cores, and that's it. Memory is reserved but not simulated as use. Nothing crashes. The numbers show
overload, not what overload breaks.

## How it works

- `sched/budget.go`: the pure arithmetic. `NewBudget`, `Fits`, `NeverFits`, `Take`, `EffectiveClasses`.
- `sched/store.go`: state on disk (`reservations/`, `queue/`, `history.json`, `events.jsonl`), the
  `flock`, the sampler hookup, and dead-holder reaping.
- `sched/admit.go`: `Admit` (the decision), `Step` (reap, measure growth, promote queue heads),
  `Release`, `Dequeue`.
- `sched/sampler.go`: how the host is measured. `Linux` reads `/proc`; `Static` is a fixed fake for
  tests. Bring your own by implementing `Sampler`.
- `sched/place.go`: pick a host from several budgets: one that fits now (most free cores), else
  the shortest queue per core.
- `sim/`, `cmd/fleetsim`: the simulator. `cmd/fleetctl`: the host-side CLI.

State directory: `$FLEET_HOST_STATE`, default `~/.local/state/agent-fleet-scheduler`.

## Limits

- The class is a guess made before the job runs. A job that declares `light` and uses 8 cores
  holds too little until the scheduler measures it and raises it, 180 s in. Admissions made in
  that window were made against the wrong number.
- Raising a reservation can leave the host over budget. The scheduler logs `over_budget` and
  admits nothing more until things drain. It doesn't kill anything.
- One host per store. Placement across hosts (`Place`) is a function you call with budgets you
  collected; there's no network layer, no ssh, no dispatcher here.
- CPU and memory only. No disk, GPU or network accounting. Tokens are plain counts.
- Linux measurement only. Memory uses `MemAvailable`; CPU uses `/proc/stat` over one second.
- Reservation is conservative. It leaves capacity idle when jobs use less than their class.
- It doesn't start your jobs. `Step` returns the reservations it granted; you launch them.

## Tests

```
scripts/verify.sh        # build, format, vet, tests, simulator; last line is a VERIFY verdict
go test ./...            # the tests alone
```

The tests cover: concurrent admission from goroutines and from separate processes (the second
fails if the lock is removed), reaping a killed holder and a never-started reservation, token
serialisation, never-fits refusals, the dynamic budget under non-fleet load, growth raising and
learning, priority and head-of-queue blocking, placement, and the simulator's overload comparison.

## License

Apache-2.0. See `LICENSE`.
