# Quickstart

Fifteen minutes from a checkout to a pipeline that has run, a policy that has
refused something, and a control plane that has handed you a credential.

Every `bash` block on this page is executed by `TestQuickstartCommandsRunAsWritten`
in `docs/docs_test.go`, in order, in one scratch directory, and each is required
to exit 0. A quickstart nobody has run is a quickstart that does not work.

You need Go 1.26 or newer. Nothing else — no container runtime, no database, no
message broker. Dhole's single binary carries an embedded NATS server with
JetStream, a SQLite run store, filesystem object stores and an engine.

## Build the two binaries

`dhole` is the control plane and the CLI. `dhole-engine` is a standalone engine
for a machine that is not the control plane — a build Mac, a GPU box, a laptop
behind CGNAT.

```bash
# DHOLE_SRC is your checkout. In it, the default is right.
DHOLE_SRC="${DHOLE_SRC:-$(pwd)}"
go build -C "$DHOLE_SRC" -o "$PWD/dhole" ./cmd/dhole
go build -C "$DHOLE_SRC" -o "$PWD/dhole-engine" ./cmd/dhole-engine
export PATH="$PWD:$PATH"
dhole version
```

`make build` does the same for `dhole` alone, and stamps the version and commit
into the binary through `-ldflags`.

## Write a pipeline

A pipeline is a typed DAG. Steps declare their inputs and outputs, and the graph
is derived from that data flow — there is no ordering to author and no shared
workspace to inherit ([ADR 0001](../.procoder/adr/0001-typed-content-addressed-dag-replaces-the-shared-mutable.md)).
The single edge below is the only reason `greet` runs before `shout`.

```bash
mkdir -p hello
cat > hello/pipeline.yaml <<'YAML'
id: hello
tenant:
  id: default
steps:
  - id: greet
    name: write a greeting
    # `command:` is a placeholder scheme carrying the argument vector and
    # environment directly. Real pipelines name a plugin the catalog resolves;
    # see docs/writing-a-plugin.md.
    plugin_ref: 'command:{"args":["/bin/sh","-c","printf %s \"$GREETING\" > out"],"env":{"GREETING":"hello"}}'
    effect_class: EFFECT_CLASS_PURE
    outputs:
      - name: out
        type:
          blob:
            media_type: text/plain
  - id: shout
    name: shout it
    plugin_ref: 'command:{"args":["/bin/sh","-c","tr a-z A-Z < in > out"]}'
    effect_class: EFFECT_CLASS_PURE
    inputs:
      - name: in
        type:
          blob:
            media_type: text/plain
    outputs:
      - name: out
        type:
          blob:
            media_type: text/plain
edges:
  - from_step: greet
    from_port: out
    to_step: shout
    to_port: in
YAML
```

`effect_class` is mandatory and it is the single input to caching and retry
([ADR 0002](../.procoder/adr/0002-effect-classes-govern-caching-and-retry.md)).
`EFFECT_CLASS_PURE` says the step has no effect beyond its declared outputs, so
it may be cached and freely retried. See [effect classes](#effect-classes-in-one-table)
below before you copy that onto a step that sends a message.

## Run it on this machine

```bash
dhole local run --state-dir ./hello/state hello/pipeline.yaml
```

`local run` is not a simulator. It starts the same embedded control plane
`dhole serve` starts — the real scheduler, a real NATS server, an engine taking
its work off the same `job.dispatch.*` work queue an engine in another
datacentre would — runs one pipeline and exits. A green local run therefore says
something about the cluster.

`--state-dir` holds the run store, the bus and the content-addressed store, and
reusing it between runs is what lets the cache hit:

```bash
dhole local run --state-dir ./hello/state hello/pipeline.yaml
```

Both steps are `pure`, so on a backend with a stable environment identity the
second run would serve them from the cache. The host-process backend has none —
it runs against whatever toolchain the host happens to carry, and no honest
digest describes that — so it reports `ErrNoStableIdentity` and its steps stay
uncacheable. That is the safe direction to be wrong in, and it is why
[the process executor](executors/process.md) is a development backend.

## Write a policy, and prove it refuses

Policy is CEL, evaluated by the same engine the control plane uses
([ADR 0019](../.procoder/adr/0019-policy-is-expressed-in-cel.md)). Every rule in
a tier must hold; a rule that returns false denies, and its id is what the
decision and the audit row name.

```bash
cat > policy.yaml <<'YAML'
revision: quickstart-1
rules:
  - id: signed-plugins-only
    expression: input.signed
    reason: this tier runs only plugins with a verified signature
  - id: no-privilege-on-untrusted-data
    expression: '!(input.tainted && "PRIVILEGED" in input.capabilities)'
    reason: a step reading untrusted data may not ask for privilege
YAML
```

`dhole policy test` evaluates one hypothetical step, locally, against no server.
The exit code is the answer, so it gates in CI:

```bash
# A signed, unprivileged step: allowed.
dhole policy test --policy policy.yaml --signed

# The same step, privileged, acting on a webhook body: refused. Naming a taint
# source is what makes the step tainted — there is no run in which data is
# untrusted by nobody. --expect-allow=false makes the refusal the passing
# outcome, which is how you assert a policy still says no.
dhole policy test --policy policy.yaml --signed \
  --capability PRIVILEGED --taint-source git-webhook --expect-allow=false
```

[Policy authoring](policy.md) has the full variable set and the rules that are
worth writing first.

## Start the control plane

`dhole serve` with no flags is the single binary: embedded bus, SQLite store,
filesystem object stores, one engine beside the control plane, and the API on
`127.0.0.1:7777`.

The API has no unauthenticated call — one contract serves the GUI, the CLI and
agents, and all three authenticate
([ADR 0013](../.procoder/adr/0013-one-api-contract-serves-gui-cli-and-agents-equally.md)).
So the plane mints a bootstrap service token at start-up, prints it once, and
writes it mode 0600 to `<blob-root>/bootstrap.token`:

```bash
mkdir -p plane
dhole serve \
  --store-dsn ./plane/dhole.db \
  --blob-root ./plane \
  --api-addr "127.0.0.1:${DHOLE_API_PORT:-7777}" > plane/serve.log 2>&1 &
echo $! > plane/serve.pid

# Wait for the credential to land rather than sleeping a guess.
for _ in $(seq 1 200); do
  [ -s ./plane/bootstrap.token ] && break
  sleep 0.1
done
test -s ./plane/bootstrap.token

export DHOLE_TOKEN="$(cat ./plane/bootstrap.token)"
export DHOLE_SERVER="$(sed -n 's/.*API \(http:[^ ]*\).*/\1/p' plane/serve.log | head -1)"
echo "control plane on $DHOLE_SERVER"
```

The credential is an ordinary service token: the store keeps only its SHA-256,
it expires in 24 hours, and it is authenticated by exactly the code path every
other credential is.

Ask the plane to validate the definition you just wrote. It will refuse it, and
the refusal is the useful part — `command:` is a placeholder scheme, and the
catalog resolves `namespace/name@version`:

```bash
if dhole pipeline validate --file hello/pipeline.yaml; then
  echo "the catalog resolved the definition"
else
  echo "validate reported diagnostics, which is what it is for"
fi
```

That gap is real and deliberate: `local run` accepts `command:` so a pipeline can
run before the plugin catalog is populated, and `pipeline validate` holds a saved
definition to the catalog. See [writing a plugin](writing-a-plugin.md).

Stop the plane. SIGTERM stops it rather than killing it: unacknowledged
dispatches go back to the queue and unsent outbox rows are still owed.

```bash
kill "$(cat plane/serve.pid)"
wait "$(cat plane/serve.pid)" 2>/dev/null || true
```

## Mint a credential without a running plane

`dhole token issue` is a local administrative command — it takes `--store-dsn`
rather than `--server`, because the contract has no identity service yet and
inventing one only the CLI could reach would be the ADR 0013 mistake with the
CLI in the privileged seat.

```bash
dhole token issue \
  --store-dsn ./plane/dhole.db \
  --tenant default --subject ci --ttl 720h
```

The token is printed once and stored only as a SHA-256. It cannot be recovered
later, only replaced.

## Effect classes in one table

| Class                       | Cached | Retried                  | For                                |
| --------------------------- | ------ | ------------------------ | ---------------------------------- |
| `EFFECT_CLASS_PURE`         | yes    | freely                   | compiles, transforms, tests        |
| `EFFECT_CLASS_IDEMPOTENT`   | never  | yes, same key            | `PUT`s, upserts, tag pushes        |
| `EFFECT_CLASS_AT_MOST_ONCE` | never  | never, waits for a human | sending a message, charging a card |

An unspecified class is refused where a manifest is published, because every
choice of default is wrong for one of those three.

## Where to go next

- [Writing an engine](writing-an-engine.md) — the contract, then the conformance suite.
- [Writing a plugin](writing-a-plugin.md) — manifests, capabilities, and the resolver.
- [Policy authoring](policy.md) — the CEL variables and the rules worth having.
- [Deployment topologies](deployment.md) — laptop, homelab, cluster.
- [Upgrades and version skew](upgrades.md) — what N and N-1 buys you.
- [Step types](steps/), [triggers](triggers/) and [executors](executors/) — one page each.
