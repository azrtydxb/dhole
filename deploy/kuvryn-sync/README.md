# dhole on kw with Kuvryn Sync

`main:deploy/kuvryn-sync/kw/resources.yaml` is the authoritative kw deployment.
Sync renders it as YAML, reconciles automatically and checks rollout health.
The Helm chart/older deployment files remain useful for other installations;
they are not the kw release source after this handover.

The initial manifests preserve the live configuration captured on 2026-09-27.
Explicit manifests avoid Helm credential generation and offline `lookup`
behavior changing production state. There are no Secret values in Git.

## Release changes

1. Build and publish the intended image, then verify it is pullable from kw's
   registry endpoint. Use an immutable `repository@sha256:...` reference for new
   application releases. Do not infer a deployment version from repository HEAD.
2. Edit `kw/resources.yaml`, preserving runtime-owned fields described below.
   For Kuvryn AI, update both the server and web-assets init container together.
3. Run the server-side dry run and review the live diff privately (live object
   annotations may contain historical credentials):

   ```sh
   kubectl --context kw -n dhole apply --server-side --dry-run=server \
     --field-manager=kuvryn-sync -f deploy/kuvryn-sync/kw/resources.yaml
   kubectl --context kw -n dhole diff --server-side \
     --field-manager=kuvryn-sync -f deploy/kuvryn-sync/kw/resources.yaml
   ```

4. Commit and push the reviewed deployment change to `main`. Sync fetches it;
   verify `Healthy` / `Synced` and desired/deployed revision equality:

   ```sh
   kubectl --context kw -n dhole get applications.sync.kuvryn.io dhole
   kubectl --context kw -n dhole get deploy,sts,cluster.postgresql.cnpg.io
   ```

Build and image promotion are explicit steps, not an automatic consequence of
merging application code. Revert the deployment commit for a rollback; image
rollback does not reverse database migrations. Do not run Helm upgrades,
legacy manifest applies or `kubectl set image` alongside Sync.

## Ownership

Sync uses a namespace-scoped service account. It cannot read or write Secrets,
change runtime RBAC, delete PVCs, or mutate other namespaces. Existing runtime
service accounts/roles remain operator-owned. CNPG retains its generated pods,
services, Secrets and PVCs; Sync owns only its parent Cluster. CNPG Cluster and
stateful configuration have prune protection and no corresponding delete
permission. The Application has `deletionPolicy: Orphan`.

The existing PostgreSQL, NATS and control-plane state volumes use `emptyDir`.
This migration preserves their pod templates and running pods. A pod replacement
can still lose local data; Sync does not make ephemeral storage persistent.
PostgreSQL backup was taken before adoption. Moving databases/bus state to
persistent storage is a separate migration requiring backup/restore and a
maintenance window. Do not change their images or pod templates casually.

The deployer has no delete permission in this namespace. PostgreSQL and NATS
also carry `sync.kuvryn.io/prune: disabled`. Removing another resource from Git
requires an explicit operator deletion instead of automatic pruning. Existing
engine sandbox RBAC, credentials and trust-tier configuration are preserved.

## Adoption and recovery

Bootstrap `rbac.yaml` and `repository.yaml` separately from the desired workload
directory. On first adoption, back up databases, Secrets, live objects with
managed fields, PVC identities and Helm records privately. Verify the rendered
server-side dry-run spec diff before transferring ownership.

Transfer only selected legacy deployment managers (Helm or old kubectl apply /
set-image managers) on the desired objects. Preserve runtime and status managers;
in particular preserve Kuvryn AI's application-managed storage fields. Apply
the reviewed desired state once as field manager `kuvryn-sync`, verify rollout,
then apply `application.yaml`. Subsequent reconciliations use
`conflictPolicy: fail`; do not leave force-adoption enabled.

Archive and retire only legacy Helm release-record Secrets after Sync becomes
Healthy. Never run `helm uninstall`, which would delete running resources.
Runtime Secrets and resources excluded from Sync remain in place.

This directory is not a disaster-recovery backup. Restore database data, Secrets,
storage claims and runtime RBAC separately before reapplying the deployment.
Suspending Sync does not itself authorize a second deployment owner.
