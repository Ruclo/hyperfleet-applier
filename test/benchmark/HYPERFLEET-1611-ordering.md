# Apply and delete ordering

One reconcile pass was timed for parallel apply and delete loops against one loop that finishes every apply and then runs deletes. The recommendation is to serialize apply-then-delete. At 100 and 1,000 desires the shared Kubernetes client rate limit already sets the wall-clock time, and serialization closes the race where a stale apply recreates an object after its delete was confirmed.

Jira does not render the relative image paths below. Attach `wall-clock.png` and `delete-finished.png` to the ticket.

## What ran

Each cell is one pass over a management-cluster partition. The partition holds `n` desires in total, half ApplyDesires and half DeleteDesires, on disjoint ConfigMap names. The ApplyDesires have no object yet, so each one is a server-side apply that creates it. The DeleteDesires still have apiserver work, so each one is a GET, a DELETE, and a confirming GET. Both loops share one dynamic client at 20 QPS and a burst of 40.

The matrix is three backends, two orderings, and three sizes.

- **envtest** is a real kube-apiserver and etcd. Request counts are two higher than the formula below because of discovery.
- **mock** is an httptest server that answers immediately, so the client rate limit is the only wait.
- **degraded** is the same mock with a seeded sleep of about 120 ms on every call (median about 100 ms, p99 about 400 ms). The loop issues about 8 requests per second and never reaches the 20 QPS cap.

Orderings:

- **parallel** starts the apply pass and the delete pass together and waits for both.
- **serial** finishes every apply, then runs the deletes.

Sizes are 10, 100, and 1,000 desires. Call count is about `2n`: one call per apply desire and three per delete desire, so 20 calls at 10 desires, 200 at 100, and 2,000 at 1,000.

Envtest, mock, and degraded were run for both orderings at all three sizes.

## Results

Wall-clock seconds for that single pass. At these sizes, time until the delete pass returns matches wall-clock, because the delete side has three calls per desire and is still running when apply finishes. For a serial pass, that delete-finished time includes the wait for every apply.


| Backend  | Ordering | 10    | 100    | 1,000  |
| -------- | -------- | ----- | ------ | ------ |
| envtest  | parallel | 0.011 | 10.00  | 100.00 |
| envtest  | serial   | 0.017 | 10.00  | 100.00 |
| mock     | parallel | 0.003 | 8.00   | 98.00  |
| mock     | serial   | 0.004 | 8.00   | 98.00  |
| degraded | parallel | 1.59  | 20.20  | 184.8  |
| degraded | serial   | 3.10  | 24.39  | 237.6  |

Once the pass is larger than the 40-call burst, wall-clock time for envtest and mock is `(http_count - 40) / 20` seconds for both orderings. At 100 desires that is 8.00 seconds on the mock and 10.00 seconds on envtest, either way. At 1,000 desires it is 98.00 seconds on the mock and 100.00 seconds on envtest. Ten desires is inside the burst, so serial is slower there: envtest is 0.011 seconds parallel and 0.017 seconds serial.

Degraded never reaches 20 QPS. Parallel wall-clock tracks the delete pass. Serial is the sum of the two passes, because the apply calls finish before the deletes start: 184.8 seconds parallel and 237.6 seconds serial at 1,000 desires.

QPS of 20/40 was chosen as 2x number of maximum **concurrent** reconcilliations. This does not imply parallel reconcilliations.
## Other implementations

ROSA and ARO use one identity for the apply desire and the delete desire. The two operations cannot be in flight for the same object, so this interleaving cannot recreate a deleted object.

GCP keeps three identities, for apply, delete, and read, and does not serialize apply against delete. The loops can interleave, and a stale apply can recreate an object after its delete has been confirmed.

HyperFleet apply and delete desires are different identities. `Identity.Type` is part of the key (`pkg/desire/types.go`), so an apply desire and a delete desire for the same ConfigMap are not the same record. There is no single desire identity to order on. Per-identity ordering is not available the way ROSA and ARO get it for free.

## Recommendation

Run one loop: finish the apply pass, then run deletes. The shared client QPS already bounds the pass, so the parallel loops are not buying wall-clock time at 100 and 1,000 desires, and one loop closes the stale-listing race.

Reapply still has to happen, or drift is never repaired. `ReasonApplied` means the apiserver accepted the last server-side apply. It does not mean the live object still matches. Skipping every later pass while `Successful=True` would leave a deleted or edited object wrong until the spec changes. How often to reapply is not decided. The candidates are:

- Apply when some time has elapsed since the last apply.
- Apply every Xth desire in the list, so a full sweep still happens over several passes.



## Follow-up

1. One loop that applies, then deletes.
2. After the reapply rule is chosen: stop sending a server-side apply for every ApplyDesire on every pass.

