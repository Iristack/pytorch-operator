# PyTorchJob PodGroup backends

This fork adds `--podgroup-backend=legacy|volcano` to the standalone PyTorch
Operator. It does not convert PyTorchJob into VolcanoJob. The Operator still
manages training Pods, services and job status; the selected scheduler assigns
resources. This patch is based on upstream `e1235d0535d88292460cb5772890bde9e9ef160c`.

## Configuration

Keep all existing Operator arguments and append:

```yaml
- --enable-gang-scheduling=true
- --gang-scheduler-name=volcano
- --podgroup-backend=volcano
```

`legacy` (the default) uses `scheduling.incubator.k8s.io/v1alpha1`.
`volcano` uses `scheduling.volcano.sh/v1beta1`. The scheduler name is independent
of the API backend: older Volcano installations may use the legacy API. There
is no automatic API fallback. With gang disabled, new jobs do not access either
PodGroup API. Existing pinned jobs continue using their recorded backend.

Install the matching scheduler and CRDs separately. Grant the Operator service
account PodGroup permissions in both API groups using `manifests/podgroup-rbac.yaml`
(or the Arena chart). The two-group read permission is required to safely inspect
pre-upgrade jobs; granting a permission does not install the old CRD. No Queue
write permission is required. Existing PyTorchJob `patch` permission is also
required to persist backend selection. For Arena, use its Helm RBAC and set the
binding namespace to `arena-system` only if applying the standalone example.

## Compatibility and ownership

Before creating children, the Operator persists these Job annotations using a
resourceVersion-guarded metadata patch:

- `arena.kubeflow.org/podgroup-backend`
- `arena.kubeflow.org/podgroup-scheduler`

Do not edit these annotations on live jobs. Restarting the Operator or changing
its default backend does not switch pinned jobs. For an unpinned job, an existing
PodGroup is adopted only when its controller owner matches the Job UID. If both
APIs contain a group, ownership conflicts, or API reads are forbidden/unavailable,
reconciliation stops with an event. If adopting a different backend without any
Pods, explicitly configure the original backend/scheduler rather than inferring
its scheduler. Discovery of a group does not prove its scheduler is running.

Missing groups can be repaired for existing Volcano Pods only when schedulerName
and `scheduling.k8s.io/group-name` agree. A Volcano template specifying a different
scheduler is rejected before creating any Pod. Legacy templates retain their old
warning-and-preserve scheduler behavior. No running Pod is migrated or deleted
just to change backends.

The Operator ensures the PodGroup before creating new Pods. Authorization errors
are never treated as NotFound. Create races re-read and verify ownership; deletion
checks the owner and supplies a UID precondition. Updating minMember preserves
queue and other fields. New groups use the scheduler's default queue; per-job
queue selection is outside this patch.

`minMember` remains the total Master + Worker replica count, including for
single-node jobs. This is gang admission, not a minimum-running-replica SLA, elastic
training policy, checkpoint recovery, or a guarantee of progress after preemption.

## Build and test

```sh
go test -mod=vendor ./pkg/scheduling -count=1
go test -mod=vendor ./pkg/controller.v1/pytorch -run 'TestGang' -count=1
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -o bin/pytorch-operator.v1 ./cmd/pytorch-operator.v1
docker build -t YOUR_REGISTRY/pytorch-operator:podgroup-compat .
```

The new API and controller regression tests use local HTTP servers; they need
permission to bind loopback ports. On Go 1.26.4, the pre-existing full controller
suite can panic in `TestCopyLabelsAndAnnotation` after a test has completed. This
was also reproduced on the unmodified e1235d0 baseline; the targeted commands above
pass.

The Dockerfile now builds from vendored dependencies. The existing runtime base
image must be accessible to your builder. Publish the built image to your own
registry and set Arena's `pytorch.imageOverride` to that immutable tag/digest.

## Rollout and rollback

Test first in an isolated namespace/controller scope, with no overlap with another
Operator watching the same jobs. Start with a one-Master job, then a multi-replica
job. Check PodGroup owner, minMember, Pod scheduler/group annotation, actual binding,
completion and cleanup. Test both backends against their matching schedulers.
HTTP regression tests are not a replacement for this cluster acceptance test.

Before production rollout, inspect running jobs and existing groups. Replace the
Operator with the same leader-election identity and one desired replica; never run
a second independently elected controller over the same jobs. Keep PodGroup RBAC
while pinned jobs remain, even when disabling gang for new jobs.

Rolling back to an unpatched image is not a transparent rollback: it ignores the
new annotations and always uses the old API. Finish or explicitly migrate new-API
jobs first. Do not remove the Volcano CRD while such jobs remain.
