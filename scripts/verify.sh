#!/usr/bin/env bash
# Runs every gate in order. The last stdout line is the verdict.
set -u
cd "$(dirname "$0")/.."
gates=0
fail() { echo "VERIFY {\"verdict\":\"fail\",\"failed\":\"$1\"}"; exit 1; }
step() {
  local name=$1; shift
  echo "== $name"
  "$@" || fail "$name"
  gates=$((gates + 1))
}
fmt_clean() { test -z "$(gofmt -l .)" || { gofmt -l .; return 1; }; }
sim_runs() {
  go run ./cmd/fleetsim -scenario examples/incident.json >/dev/null &&
    go run ./cmd/fleetsim -scenario examples/three-hosts.json >/dev/null
}
step install go mod download
step build go build ./...
step format fmt_clean
step vet go vet ./...
step test go test -count=1 ./...
step simulate sim_runs
echo "VERIFY {\"verdict\":\"pass\",\"gates\":$gates}"
