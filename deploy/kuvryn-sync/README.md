# dhole on kw with Kuvryn Sync

`main:deploy/kuvryn-sync/` is the authoritative kw deployment configuration.
Sync manages deployment images/configuration, application credentials, runtime
permissions and persistent claims (directly or through an operator parent).
Older Helm charts and deployment files are not the kw release source.

| Application / namespace | Desired directory |
| --- | --- |
| dhole | `kw` |

Each Application uses automatic reconciliation, self-healing, health checks and
`conflictPolicy: fail`. Failed rollouts can roll back to a prior healthy revision;
image rollback does not undo database changes. Application deletion orphans
resources. Databases, persistent claims, credentials and runtime permissions
have prune protection. Namespace-scoped deployer permissions cover the desired
resources; named cluster-RBAC permissions cover only the adopted runtime roles.

## Release a change

1. Build/publish an image and verify it is pullable from kw. Update the intended
   container reference to an immutable `repository@sha256:...` in the desired
   file. Merging application code alone does not promote an image.
2. Edit configuration in the relevant desired directory. For credentials, use
   `sops edit <file.sops.yaml>` with the namespace's age identity. Never commit a
   decrypted file, put credentials in shell arguments, or rotate keys as part
   of a routine deployment.
3. Render privately and validate against the API using the Sync field manager:

   ```sh
   export SOPS_AGE_KEY_FILE="$HOME/.config/kuvryn-sync/age/kw-dhole.agekey"
   python3 deploy/kuvryn-sync/render-private.py kw /tmp/dhole-desired.yaml
   kubectl --context kw apply --server-side --dry-run=server \
     --field-manager=kuvryn-sync -f /tmp/dhole-desired.yaml
   ```

   Review the diff privately: rendered credentials and old live annotations
   must not appear in logs or Git. Delete the private render when finished.
   For other Applications, use its directory and corresponding namespace key.
4. Commit and push the reviewed files to `main`; Sync polls within 60 seconds.
   Verify every affected Application is `Healthy` / `Synced`, its deployed
   revision equals the desired revision, and workloads are ready.

Do not run Helm upgrades, legacy manifest applies or `kubectl set image`
alongside Sync. Revert the deployment commit to roll back. A field conflict
requires resolving the competing writer; do not enable blanket force adoption.

## Bootstrap and recovery

The namespace, Sync deployer identity/permissions, Repository/Application
objects, Git access and age decryption keys bootstrap reconciliation. They are
represented by the top-level definitions; credentials and private age keys are
kept outside Git. Do not place bootstrap definitions inside a desired directory.
Runtime RBAC is in the desired directories, separate from this deployer RBAC.

Apply each `*-rbac.yaml`, Repository and Application pair for the additional
namespaces (or `rbac.yaml`, `repository.yaml`, `application.yaml` for the main
namespace). Existing named cluster roles/bindings must be bootstrapped before
adoption: the deployer can update only these names, not create arbitrary
cluster roles. Use the desired definitions to restore those names as an admin.

Application credential values are SOPS-encrypted with a namespace-specific age
recipient. `kuvryn-sync-sops` holds the private key in each Application namespace
and has `sync.kuvryn.io/decryption-key: "true"`. Operator recovery copies live at
`~/.config/kuvryn-sync/age/kw-<namespace>.agekey` (mode 0600). Back them up securely;
Git alone cannot recover the credentials.

Private repositories follow the established Sync HTTPS credential setup because
GitHub disables deploy keys. `<namespace>-sync-git` uses the existing Sync Git
credential (also used by Novamem). Rotate the copies together. This credential
is not asserted to be read-only, but no image-update/push feature is enabled.
Dhole's public Repository requires no Git credential.

Restore database data, claims, bootstrap keys and operator installations before
starting a fresh deployment. CNPG and cert-manager retain their generated child
resources; Sync manages the declarative parents. Do not adopt transient pods,
jobs, user workspaces, test fixtures or the namespace's default ServiceAccount.

## Migration record

The 2026-09-27 adoption captured live configuration and image identities, backed
up databases/Secrets/Helm records privately, verified server-side dry runs and
SOPS decryption round-trips, and transferred only deployment field ownership.
Secrets and bound volume identities were preserved. Old Helm release-record
Secrets were archived and retired after health verification; `helm uninstall`
was never used. Initial manifests are explicit YAML to avoid credential-generating
Helm lookups changing state during reconciliation.

## Existing ephemeral storage

PostgreSQL, NATS and control-plane local state currently use `emptyDir`. This
migration changes no pod templates and preserves running pod identities. Sync
does not make that storage persistent: replacing those pods can lose local
data. PostgreSQL was backed up before adoption. Persistent storage needs a
separate backup/restore migration. Do not casually change these pod templates.

The deployer has no delete permission in this namespace. Datastores and
credentials also have prune protection. Removing another resource from Git
requires explicit operator deletion. Engine sandbox RBAC, credential values,
images and trust-tier settings are preserved. The `dhole-deploy` TrueNAS
maintenance identity and test fixtures are not runtime application resources.
