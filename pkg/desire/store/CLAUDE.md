# pkg/desire/store

Backend implementations of the `desire.SpecStore` / `desire.StatusStore` contracts defined in
`pkg/desire` (see `pkg/desire/CLAUDE.md`).

- **memory** — single-process, mutex-guarded map; used in unit tests and as the store passed to
  `Reconciler` in envtest tests.
- **redis** — one JSON-serialized `resourceRecord` per resource key, mutated under
  WATCH/MULTI/EXEC for compare-and-swap on `Version` (`casMutate`, bounded by `maxCASRetries`).
- **conformance** — `RunSpecStoreSuite`/`RunStatusStoreSuite` exercised against *both* backends,
  since any concrete store implements both `SpecStore` and `StatusStore`. Add new backend behavior
  tests here, not per-backend, unless the behavior is backend-specific (e.g. Redis CAS retry
  exhaustion).
