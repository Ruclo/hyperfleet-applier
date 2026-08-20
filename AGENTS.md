# AGENTS.md

This file is the canonical agent guidance for this repository.

## What this is

Go library for the HyperFleet desire contract and store backends (see README.md for the full
mission and how this fits with Sentinel/Adapter/the API).

## Commands

Run `make help` for the full target list (build, test, lint, fmt, tidy, etc.). Notes that aren't
obvious from that listing:

- Run a single test: `go test ./pkg/desire/... -run TestName -v`.
- `make test-envtest` runs the `envtest`-tagged tests (`internal/controllers/applydesire/envtest_test.go`) against a real
  kube-apiserver via `sigs.k8s.io/controller-runtime/pkg/envtest`. These are excluded from the
  normal `make test` run (no `-tags envtest`), so `go test ./...` alone does not cover them.
- Tool binaries (golangci-lint, setup-envtest) are version-pinned in `tools/go.mod`, a separate
  module from the main one, invoked via `go tool -modfile=tools/go.mod <name>` (see the `gotool`
  helper in the Makefile). Don't add tool dependencies to the root `go.mod`.

## Architecture

- `pkg/desire` — desire types, identity/validation, and the SpecStore/StatusStore contracts. See
  `pkg/desire/CLAUDE.md`.
- `pkg/desire/store/{memory,redis,conformance}` — store backends and their shared test suite. See
  `pkg/desire/store/CLAUDE.md`.
- `internal/controllers/{applydesire,deletedesire,readdesire,util}` — the reconcile controllers and
  their shared support code (not part of the public library surface):
  - `applydesire` — the SSA `Reconciler` that drives ApplyDesires to the
    local kube-apiserver. See `internal/controllers/applydesire/CLAUDE.md`.
  - `deletedesire` — confirms a Kubernetes resource is gone (past finalizers) for DeleteDesires.
    See `internal/controllers/deletedesire/CLAUDE.md`.
  - `readdesire` — the informer-driven `Controller` that mirrors live Kubernetes object state back
    into ReadDesire status. See `internal/controllers/readdesire/CLAUDE.md`.
  - `util` — shared helpers (`WithCondition`, `Equal`, `DescribeIdentity`) reused across the
    reconcile controllers.
