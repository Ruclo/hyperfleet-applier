# internal/controller/applydesire

`Reconciler` reconciles ApplyDesires (see `pkg/desire/CLAUDE.md`) against the local kube-apiserver
via server-side apply.

`Reconciler` is bound to one management-cluster partition. `ReconcileAll` lists every ApplyDesire
for that partition and reconciles each independently - one failure is recorded on that desire's
status and does not abort the others, but every such failure is `errors.Join`-ed into the value
`ReconcileAll` returns, so the host can drive retry/backoff and expose controller health. The return
is nil only when the list succeeds and no desire failed. `ReconcileAll` depends on narrow unexported
interfaces (`specLister.ListApplyDesires`, `statusWriter.UpdateApplyDesireStatus`), not the full
`SpecStore`/`StatusStore`.

**Context cancellation** is handled apart from resource failures: it is caller-driven control flow
(e.g. shutdown), not evidence the resource failed. `ReconcileAll` checks `ctx.Err()` before each
desire, and `applyToCluster` returns the context error (rather than a `KubeAPIError` status) when an
SSA call fails with `ctx.Err() != nil`. Either way the pass aborts immediately and returns the
context error without recording any status, so healthy statuses are never overwritten during
shutdown.

Per desire, `reconcileOne`/`applyToCluster`:
1. Unmarshal `Spec.KubeContent` into an `unstructured.Unstructured`. Invalid JSON, or a manifest
   that cannot be decoded as an object with `kind` (e.g. missing `kind`), or missing
   `apiVersion`/`metadata.name` → `ReasonPreCheckFailed` (no kube-apiserver call attempted).
2. Resolve GVK → GVR via the injected `meta.RESTMapper`. Hosts should supply a
   `restmapper.DeferredDiscoveryRESTMapper` (or equivalent). After newly installed CRDs or other
   discovery changes, the host must call `Reset()` on that mapper (or recreate it) before
   retrying GVK-to-GVR resolution; otherwise stale cache misses can persist as
   `ReasonPreCheckFailed`. The reconciler does not call `Reset()` itself - mapper ownership
   stays with the host.
3. Reject with `ReasonPreCheckFailed` if the mapped GVR, name, or (for namespaced resources)
   namespace disagree with `d.Identity`. The store validates those fields independently, so a stored
   desire can disagree; applying anyway would mutate a different object than the one status is
   written against. An omitted manifest namespace is not a disagreement; apply uses
   `Identity.Namespace`. **`metadata.name` is required in the manifest** and is not backfilled from
   identity (namespace is backfilled via the client path only).
4. Apply via SSA under the single global field manager (`"hyperfleet-applier"`) with `Force: true`
   - single-writer ownership model, always reclaims contested fields. The SSA target is always
   `d.Identity` (name/namespace), not the raw manifest coordinates.
5. Write the resulting condition back through `StatusStore.UpdateApplyDesireStatus`, using the
   desire's `Version` read at list time. A `ErrVersionConflict` here means spec/status moved since
   `ListApplyDesires` - treated as a benign race, not an error; the next `ReconcileAll` pass retries.

The reconciler reads ApplyDesire intent and writes reconciliation status. It does not mutate
desire intent through the spec store. Authentication and storage-level authorization are outside
the controller's responsibility; the narrow `specLister`/`statusWriter` interfaces constrain
normal Go callers only.
`conditions.Equal` (ignoring `LastTransitionTime`) suppresses status writes that wouldn't change
anything.

## Operational semantics (MVP)

- **Polling only:** `ReconcileAll` has no internal workqueue, rate limiter, or per-desire backoff.
  The hosting binary must enforce a finite reconciliation cadence and configure client-side
  QPS/burst (or equivalent workqueue backoff) so unchanged desires cannot generate unbounded
  SSA traffic. `ReconcileAll` itself is unchanged.
- **Cost per tick:** every listed desire gets a full SSA round-trip each pass, even when spec and
  cluster are unchanged. Unchanged reconciles suppress the status-store write (and envtest proves a
  true cluster no-op on `resourceVersion`), but not the apiserver call.
- **`ReasonApplied` meaning:** success is recorded when the kube-apiserver **accepts** the SSA
  request. The applier does not GET the live object or diff it against the desired manifest.
- **Apply before status CAS:** SSA runs before `UpdateApplyDesireStatus`. If the status write loses
  a version race, the cluster may briefly hold the list-time manifest while the store has moved on;
  the next pass applies the fresh spec. The per-resource `Version` is shared across
  Apply/Delete/Read sub-states for the same Kubernetes target, so unrelated store writes can also
  bump it and trigger benign `ErrVersionConflict` on the apply status path.
- **Cluster-scoped manifests:** `checkApplyTarget` does not reject a non-empty `metadata.namespace`
  on cluster-scoped kinds; the applier forwards it in the SSA body and the kube-apiserver decides
  whether to accept or reject the request.

envtest coverage (`envtest_test.go`, build tag `envtest`, run via `make test-envtest`) includes:
unchanged reconcile cluster no-op (`resourceVersion`), `Force` field reclaim, and cluster-scoped
stray-namespace behavior against a real apiserver. Excluded from the normal `go test ./...` run.
