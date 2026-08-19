# pkg/desire/store

Backend implementations of the `desire.SpecStore` / `desire.StatusStore` contracts defined in
`pkg/desire` (see `pkg/desire/CLAUDE.md`).

- **memory** — single-process, mutex-guarded map; used in unit tests and envtest.
- **redis** — one JSON `resourceRecord` per desire, updated with WATCH/MULTI/EXEC CAS. `Create*`
  uses multi-key WATCH to enforce shared-owner and apply/delete rules atomically.
- **conformance** — `RunSpecStoreSuite` and `RunStatusStoreSuite` run against both backends. Put
  shared backend behavior tests here unless the behavior is backend-specific.
