# pkg/desire

Desire types, identity/validation rules, and the SpecStore/StatusStore contracts. Backends
implementing these contracts live in `pkg/desire/store/` (see `pkg/desire/store/CLAUDE.md`).

## Desire model

Three desire types, each targeting exactly one Kubernetes resource via an `Identity`
(managementCluster + type + group/resource/namespace/name):

- **ApplyDesire** — make a resource exist with specific content (SSA, `Force=true`)
- **DeleteDesire** — make a resource not exist (confirmed gone past finalizers)
- **ReadDesire** — mirror a live object's state back to the control plane (includes `KubeContent`)

`Identity.ResourceKey()` projects away `Type` to a `ResourceKey`, the key backends actually store
under (`desire/<cluster>/<group>/<resource>/<namespace>/<name>`, path-escaped) — one physical
resource record can hold an Apply spec, a Delete flag, and a Read status simultaneously, keyed by
the same resource key. Creating a DeleteDesire supersedes (clears) any existing ApplyDesire for that
resource; creating an ApplyDesire while a DeleteDesire is active returns `ErrDeletePending`.

`Identity` implements `slog.LogValuer` so controllers can log `"identity", id` instead of unwrapping
fields by hand (shared across apply/delete/read).

Every desire carries one summary condition (`Status.Conditions`, `TypeSuccessful`) with positive
polarity — `Successful=True` means desired state is achieved, `Successful=False` covers both
"in progress" (e.g. `ReasonWaitingForDeletion`, not an error) and real errors (`ReasonKubeAPIError`,
`ReasonPreCheckFailed`), disambiguated by `Reason`, not by a separate Progressing/Degraded condition.
This mirrors the rest of the HyperFleet status contract. See the `Conditions` doc comment in
`types.go` for the full reason table.

## Store contracts

Store contracts split spec and status:

- **SpecStore** — Create/Get/Update/Delete per desire type, plus `ListApplyDesires` /
  `ListDeleteDesires` / `ListReadDesires` and `DeleteByPrefix`. Create/Update enforce single-writer
  ownership (`ErrOwnerConflict` on mismatch) and require exact `Version` match for updates
  (`ErrVersionConflict` if stale).
- **StatusStore** - status-only Get/Update per desire type. Does **not** check ownership (any
  reconciler can write status once it holds the record). `UpdateApplyDesireStatus` and
  `UpdateDeleteDesireStatus` require an exact `Version` match; `UpdateReadDesireStatus`
  performs no version check and does not advance the shared resource version.

`Validate()` methods on `Identity`/`ApplySpec`/each desire type enforce DNS-1123/1035 sizing rules
on identity fields (`validate.go`) — backends call these at Create time.

`observe.go` holds process-local owner-conflict metrics/logging (`CheckOwner`,
`ReportOwnerConflict`); real Prometheus/OTel export belongs in the hosting binary.
