# 0013 — One API contract serves GUI, CLI and agents equally

Status: accepted
Date: 2026-09-09

## Context

Dhole must be drivable by a drag-and-drop React canvas, by a CLI, and by an AI agent
working alongside the user — with none of them second-class. The usual outcome is a rich
internal API for the web UI and a thinner, later, partial public API for everyone else,
which guarantees agents and automation are perpetually behind.

## Decision

The GUI has no privileged endpoints. It is one API client among several, and an
editor-only affordance is a bug rather than a roadmap item. This is enforced mechanically:
the API is defined in protobuf and served through ConnectRPC, which speaks gRPC and plain
JSON over HTTP from one definition and generates the Go server, the typed TypeScript
client the React app uses, and the schemas behind the MCP tool definitions and OpenAPI
docs. Internal bus messages use the same protobuf discipline.

Editing is operation-level, not document-level: `add_step`, `connect`, `set_property`,
`remove_edge`, `rename`, each taking a document version and returning the resulting diff
plus its own inverse. The GUI emits one operation per interaction; agents emit the same
ones. Two read-only endpoints serve both audiences constantly — `validate`, returning
structured diagnostics with source positions, and `plan`, a dry run showing what would
execute, what is a cache hit, and which engine each step lands on.

Plugin schemas (0012) drive the properties panel, request validation, agent tool
discovery and editor autocomplete from one source.

## Consequences

Easier: undo/redo is replaying inverses, so agents get "revert that" free. Optimistic
concurrency falls out of the version field and multiplayer editing later is an extension
rather than a rewrite. A new step type means publishing a schema, not React work plus API
work plus agent docs in three places that drift.

Harder: protobuf and code generation are in the build from day one. Operation-level
editing is more design work than PUT-the-document. Every capability must be expressible in
the public API even when a GUI-specific shortcut would be quicker, which is the point but
will feel like friction.
