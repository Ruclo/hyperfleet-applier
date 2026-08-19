# pkg/desire

Desire types, identity/validation rules, and the SpecStore/StatusStore contracts. Backends
implementing these contracts live in `pkg/desire/store/` (see `pkg/desire/store/CLAUDE.md`).

## Desire model

Three desire types, each targeting exactly one Kubernetes resource via an `Identity`
(managementCluster + type + group/resource/namespace/name):

- **ApplyDesire** — make a resource exist with specific content (SSA, `Force=true`)
- **DeleteDesire** — make a resource not exist (confirmed gone past finalizers)
- **ReadDesire** — mirror a live object's state back to the control plane (includes `KubeContent`)

Each desire type is its own record with its own `Version`, keyed by full `Identity`. A target can
therefore have up to three sibling records: apply, delete, and read.

Create-time invariants across sibling records:

- one owner per target (`ErrOwnerConflict` on mismatch)
- apply/delete mutual exclusion (`DeleteDesire` supersedes `ApplyDesire`; active delete blocks apply)
- `ReadDesire` coexists with either

`Identity` implements `slog.LogValuer` so controllers can log `"identity", id` instead of unwrapping
fields by hand (shared across apply/delete/read).

Every desire carries one summary condition (`TypeSuccessful`). `Successful=True` means achieved;
`Successful=False` covers both in-progress and failure states, distinguished by `Reason`.

## Store contracts

Store contracts split spec and status:

- **SpecStore** — Create/Get/Update/Delete per desire type, plus `ListApplyDesires` /
  `ListDeleteDesires` / `ListReadDesires` and `DeleteByPrefix`. Create/Update enforce single-writer
  ownership (`ErrOwnerConflict` on mismatch) and require exact `Version` match for updates
  (`ErrVersionConflict` if stale).
- **StatusStore** - status-only Get/Update per desire type. Does **not** check ownership.
  `UpdateApplyDesireStatus` and `UpdateDeleteDesireStatus` require exact `Version` match;
  `UpdateReadDesireStatus` does not advance `Version`.

The spec/status split is a Go capability boundary, not a storage-enforced permission boundary.

`Validate()` methods on `Identity`/`ApplySpec`/each desire type enforce DNS-1123/1035 sizing rules
on identity fields (`validate.go`) — backends call these at Create time.

`observe.go` holds process-local owner-conflict metrics/logging.
