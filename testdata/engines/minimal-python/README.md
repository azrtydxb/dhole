# The minimal Python engine

A complete Dhole engine in one file, in a language that is not Go, written
against `docs/wire-contract.md` and nothing else. It exists so that "an engine
can be written in any language" is a claim somebody has actually tested.

Run the conformance suite against it:

```sh
make conformance ENGINE="python3 testdata/engines/minimal-python/engine.py"
```

## Dependencies: none, deliberately

**Python 3.8 or newer. No `pip install`. No virtualenv. No `nats-py`, no
`protobuf`.**

That is a decision, not an accident, and it was taken twice over:

- On the machine this was written on, `pip install nats-py` is refused —
  Homebrew's Python is externally managed (PEP 668), so installing needs either
  `--break-system-packages` or a virtualenv the `make conformance` command
  would then have to know about. A conformance suite with an install step is a
  conformance suite people stop running.
- More importantly, the dependency would have hidden the thing being tested. A
  NATS client library and a generated protobuf module between the engine and
  the wire mean the suite proves those libraries work. Speaking the protocol
  directly is what makes this a demonstration that the CONTRACT is sufficient:
  every byte on the wire is produced by code you can read here, from the
  document.

So the file contains, in order:

1. A protobuf encoder and decoder — two wire types, about 80 lines.
2. A NATS client — `CONNECT`/`SUB`/`PUB`/`MSG`/`HMSG`/`PING`/`PONG` over a TCP
   socket, plus the JetStream pull consumer API (`$JS.API.CONSUMER.DURABLE.CREATE`
   and `$JS.API.CONSUMER.MSG.NEXT`) as request/reply.
3. The engine itself: register, pull dispatches, run the command with
   `subprocess`, stream logs, publish status, heartbeat, honour `Cancel`.

If you would rather have the libraries, `pip install nats-py protobuf` and
generate `dhole/v1/*_pb2.py` with `buf generate` — the engine would be a third
of the length. Nothing in the contract requires either approach.

## What the engine is given

None of this is in the wire contract, which is the single biggest gap this
engine found: the contract says what an engine says on the bus, and nothing
about how an engine is configured or what the object store is. These names are
the conformance suite's convention, and an engine for a real deployment will
need its own.

| Variable                | Meaning                                               |
| ----------------------- | ----------------------------------------------------- |
| `DHOLE_NATS_URL`        | Bus to dial. Engines are outbound-only.               |
| `DHOLE_ENGINE_ID`       | Identity to register under; names its control subject |
| `DHOLE_ENGINE_TIER`     | Trust tier — decides which dispatch subjects it takes |
| `DHOLE_BLOB_DIR`        | Object store root; a key is a path under it           |
| `DHOLE_DISPATCH_STREAM` | JetStream work queue holding dispatches               |
| `DHOLE_SECRET_SUBJECT`  | Request/reply subject that redeems a secret handle    |
| `DHOLE_ENGINE_SLOTS`    | Jobs to run at once                                   |

A step runs in a fresh working directory holding `inputs/<port>`, and whatever
it writes to `outputs/<port>` is collected. That layout is a convention too.

## `--ignore-cancel`

Makes the engine drop `EngineControl{Cancel}`. It is not a feature: it is the
broken variant `TestConformanceDetectsAnEngineThatIgnoresCancellation` runs, to
prove the suite's cancellation case can actually fail. A conformance suite that
cannot fail is worse than none, because it certifies compliance.

## What had to be guessed

Every one of these is marked `GAP` in `engine.py`. They are the places a
stranger cannot get right from the document alone:

- **The `<caps>` subject token.** The contract says "a stable hash of the sorted
  capability set". The actual algorithm is sha256 over the sorted, de-duplicated
  capability enum NUMBERS, each in decimal followed by `\n`, truncated to 16 hex
  characters. Guess differently and the engine subscribes to a subject nothing
  is published on: no work, no error, no clue.
- **The object store.** `output_prefix` and `log_key` name objects in a store
  that has no protocol anywhere in the contract.
- **Secret redemption.** "An engine redeems a handle for the value" — on what
  subject, with what message, and what does a refusal look like?
- **Step timeouts.** There is no timeout field in the schema at all.
- **The JetStream details.** The stream name, that it is a work queue, that a
  consumer is durable and filtered per capability set, and that an ack is a
  publish to the message's reply subject.
