# Changelog

## 0.1.0 - 2026-11-18

- Admission by reservation: each job declares a class (`light`, `build`, `browser`) and holds that peak for its life; a job is admitted only if every held reservation plus its own fits the host budget.
- Budget is recomputed on every admission; every read-decide-write runs under one host-wide `flock`.
- Durable priority queue with head-of-queue blocking; reservations are dropped when the holder's pid is gone or never showed up within 180 s.
- Reservations are raised to the measured peak when a job outgrows its class, and per-class peaks are learned after 3 samples.
- Jobs that can never fit are refused instead of queued forever; every decision is a JSON line in `events.jsonl`.
- `fleetctl` host-side CLI, `fleetsim` simulator with two synthetic scenarios, and `scripts/verify.sh`.
