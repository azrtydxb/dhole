# Writing a plugin

A plugin is one unit of work a step can name: a step type, a trigger type or an
engine type. It is distributed as a digest-addressed artifact and described by a
manifest the control plane can enforce against.

Two things make a plugin different from a script somebody runs. It **declares
what it needs**, so the scheduler can refuse to place it where those needs
cannot be met. And it **declares what it does to the world**, so the cache and
the retry policy can be correct rather than advisory.

## The manifest

A manifest is what a type says about itself. It is published to the catalog —
the durable registry of the types a tenant may use — and everything downstream
trusts it, so it is validated where it is published rather than guessed at when
something runs.

| Field                           | Meaning                                             |
| ------------------------------- | --------------------------------------------------- |
| `namespace`, `name`, `version`  | identity; the reference is `namespace/name@version` |
| `digest`                        | the artifact's content digest, `sha256:<hex>`       |
| `kind`                          | `step`, `trigger` or `engine`                       |
| `effect_class`                  | mandatory; governs caching and retry                |
| `capabilities`                  | what the sandbox must grant                         |
| `input_schema`, `output_schema` | JSON Schema for the typed ports                     |
| `engine_types`                  | the engine types that can run it                    |

A version is **immutable**. Publishing a different manifest under a version that
already exists is refused; republishing byte-identical content is a no-op,
because deploys get retried. A mutable version would make every lockfile already
written point silently at different code, and every cache key folded over it a
lie ([ADR 0010](../.procoder/adr/0010-runtime-engine-registry-is-separate-from-the-durable-catalog.md)).

## Effect class: the one field to get right

`effect_class` is the single input to caching and retry
([ADR 0002](../.procoder/adr/0002-effect-classes-govern-caching-and-retry.md)).
There is no default, because every candidate default is wrong for one of the
three populations:

- `EFFECT_CLASS_PURE` — no observable effect beyond its declared outputs.
  Cacheable, freely retryable. Compiles, transforms, tests.
- `EFFECT_CLASS_IDEMPOTENT` — has an external effect, but repeating it with the
  same idempotency key is indistinguishable from running it once. Retried, never
  cached. Upserts, `PUT`s, tag pushes.
- `EFFECT_CLASS_AT_MOST_ONCE` — an external effect that cannot be repeated
  safely. Never auto-retried; a failure waits for a person. Sending a message,
  charging a card, cutting a release.

Declaring `pure` on a step that sends a message is how "send the message" gets
silently replayed. Declaring `at-most-once` on a compile costs you the cache and
a night of somebody clicking retry. Neither is caught by a test; both are caught
by reading this table before you publish.

## Capabilities

A capability is a permission the step requires and an engine advertises. The
scheduler matches the two; policy decides whether the step may ask at all.

| Capability              | Grants                                   |
| ----------------------- | ---------------------------------------- |
| `CAPABILITY_NETWORK`    | outbound network from inside the sandbox |
| `CAPABILITY_SECRETS`    | may redeem secret references             |
| `CAPABILITY_PRIVILEGED` | runs with elevated privileges            |
| `CAPABILITY_HOST_MOUNT` | may mount a path from the engine host    |

Declare the smallest set that works. A step declaring `PRIVILEGED` because it
might one day need it is a step that policy will refuse in every tier that has
thought about privilege, and a step that runs nowhere is worse than one that
asks for less.

A step never receives secret **values**. It receives short-lived references it
redeems, which is what keeps a secret out of the dispatch, out of the bus, and
out of any log that captured one.

## Referencing a plugin

A reference is scheme-addressed, and one resolver serves every scheme
([ADR 0011](../.procoder/adr/0011-plugin-artifacts-resolve-through-one-scheme-addressed.md)):

- `oci://registry/repo:tag` — an OCI artifact in a registry.
- `cas://sha256:<hex>` — bytes in the content-addressed store.

An unknown scheme is refused rather than defaulted, because defaulting would
fetch code from somewhere the author never named.

**A tag is resolved to a digest exactly once**, when a definition is saved, and
never again. Dispatch fetches the digest that resolution recorded and ignores
whatever the tag points at today. This is the property that makes a re-run of a
year-old pipeline run the code it ran the first time — and it is why fetching a
reference whose digest was never recorded is refused outright rather than
resolved on the spot.

`builtin:` names the step types the control plane runs itself —
[`builtin:loop`](steps/loop.md) and [`builtin:agent`](steps/agent.md). They are
not plugins the registry resolves, because their bounds have to be enforced
somewhere no tenant-supplied code can reach.

`command:` carries an argument vector and environment inline. It is a
placeholder for development — [the quickstart](quickstart.md) uses it — and the
catalog does not resolve it, so a definition that names it validates as a
diagnostic against a saved pipeline.

## Signing and upstreams

An artifact may carry a cosign signature, and whether an unsigned artifact may
run at all is a **policy** question rather than a hard-coded one: `input.signed`
and `input.upstream` are variables a rule reads. See
[policy authoring](policy.md).

Federated upstreams are registered per tenant, each with the namespace it may
claim and the policy it is subject to. A namespace local plugins are addressed
under cannot be claimed by an upstream.

## Checklist before publishing

1. The effect class is the honest one, not the convenient one.
2. The capability set is the smallest that works.
3. Input and output schemas exist and have `$id`s — a structured port's
   compatibility is decided by matching `$id`, and that is what lets the editor
   reject a bad edge before a run starts.
4. `engine_types` names engines that actually exist in the deployment.
5. The version has never been published with different content.
