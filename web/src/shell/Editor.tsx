/**
 * The editor screen: the shell, filled in from the control plane.
 *
 * Everything shown here is something the plane actually said. The design this
 * implements shows a plugin catalog, a run history and an assistant transcript
 * as well; the contract has no RPC behind any of those yet, so their panes
 * render an empty state that names what is missing instead of a convincing
 * list of things that do not exist. A demo is allowed to invent data. An
 * editor somebody runs a deploy from is not.
 */
import { useMutation, useQuery } from "@tanstack/react-query";
import { useCallback, useMemo, useState } from "react";

import { engineClient, pipelineClient } from "../api/client.js";
import { useTheme } from "../design/useTheme.js";
import { Canvas } from "../canvas/Canvas.js";
import { Diagnostics, type Diagnostic } from "./Diagnostics.js";
import { Inspector, type EffectClass } from "./Inspector.js";
import { MenuBar, type Menu } from "./MenuBar.js";
import { Shell } from "./Shell.js";
import { Sidebar } from "./Sidebar.js";
import { StatusBar, type EngineStatus } from "./StatusBar.js";
import { Toolbar, type ToolbarAction } from "./Toolbar.js";
import { asPortTypeName } from "../design/PortGlyph.js";
import { EffectClass as EffectClass_ } from "../gen/dhole/v1/common_pb.js";

const menus: readonly Menu[] = [
  {
    id: "file",
    label: "file",
    items: [
      { id: "save", label: "save revision", shortcut: "⌘S" },
      { id: "revisions", label: "revision history…" },
    ],
  },
  {
    id: "edit",
    label: "edit",
    items: [
      // Undo is listed and disabled rather than hidden. Every edit is an
      // operation with an exact inverse (ADR 0020), so undo is a thing this
      // editor will have; pretending the concept does not exist would be the
      // more confusing lie.
      { id: "undo", label: "undo", shortcut: "⌘Z", disabled: true },
      { id: "redo", label: "redo", shortcut: "⇧⌘Z", disabled: true },
    ],
  },
  {
    id: "view",
    label: "view",
    items: [
      { id: "theme", label: "toggle theme" },
      { id: "fit", label: "fit pipeline to view" },
      { id: "diagnostics", label: "toggle diagnostics" },
    ],
  },
  {
    id: "admin",
    label: "admin",
    items: [{ id: "engines", label: "engines…" }],
  },
];

/** effectOf maps the wire enum onto the word the inspector shows. It is a
 * switch on the generated enum rather than an index into an array, because an
 * array silently reorders and this value decides whether a step may be cached
 * or retried. */
function effectOf(value: EffectClass_): EffectClass {
  switch (value) {
    case EffectClass_.PURE:
      return "pure";
    case EffectClass_.IDEMPOTENT:
      return "idempotent";
    case EffectClass_.AT_MOST_ONCE:
      return "at-most-once";
    default:
      return "pure";
  }
}

const categories = [
  "all",
  "trigger",
  "scm",
  "build",
  "test",
  "deploy",
  "llm",
  "flow",
] as const;

export function Editor({
  pipelineId,
  revisionId,
  tenant,
  user,
}: {
  readonly pipelineId: string;
  readonly revisionId: string;
  readonly tenant: string;
  readonly user: string;
}) {
  const { theme, toggleTheme } = useTheme();
  const [tab, setTab] = useState<"properties" | "logs">("properties");
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(true);
  const [selected, setSelected] = useState<string | null>(null);
  const [category, setCategory] = useState<string>("all");
  const [query, setQuery] = useState("");
  const [notice, setNotice] = useState("");

  const pipeline = useQuery({
    queryKey: ["pipeline", pipelineId, revisionId],
    queryFn: () => pipelineClient.getPipeline({ pipelineId, revisionId }),
  });

  const engines = useQuery({
    queryKey: ["engines"],
    queryFn: () => engineClient.listEngines({}),
    // The fleet changes without this tab doing anything, and an engine that
    // went offline five minutes ago is exactly the thing the status bar is for.
    refetchInterval: 15_000,
  });

  const validate = useMutation({
    mutationFn: () => pipelineClient.validate({ pipelineId, revisionId }),
  });

  const plan = useMutation({
    mutationFn: () => pipelineClient.plan({ pipelineId, revisionId }),
  });

  const start = useMutation({
    mutationFn: () => pipelineClient.startRun({ pipelineId, revisionId }),
  });

  const revision = pipeline.data?.revision;
  const steps = useMemo(
    () => pipeline.data?.pipeline?.steps ?? [],
    [pipeline.data],
  );

  const selectedStep = useMemo(
    () => steps.find((step) => step.id === selected) ?? null,
    [steps, selected],
  );

  /** Diagnostics come from Validate, plus the one thing validate cannot know:
   * whether the engines a step is pinned to are actually up. */
  const diagnostics: readonly Diagnostic[] = useMemo(() => {
    const fromValidate: Diagnostic[] = (validate.data?.diagnostics ?? []).map(
      (diagnostic, index) => ({
        id: `validate-${index}`,
        severity: diagnostic.severity === "warning" ? "warning" : "error",
        text:
          diagnostic.port === ""
            ? diagnostic.message
            : `${diagnostic.stepId}.${diagnostic.port} : ${diagnostic.message}`,
        source: "validate",
        // Spread rather than set to undefined: the project compiles with
        // exactOptionalPropertyTypes, where an absent field and a field
        // holding undefined are different things.
        ...(diagnostic.stepId === "" ? {} : { stepId: diagnostic.stepId }),
      }),
    );
    const offline: Diagnostic[] = (engines.data?.engines ?? [])
      .filter((engine) => engine.state === "gone")
      .map((engine) => ({
        id: `engine-${engine.id}`,
        severity: "warning" as const,
        text: `engine ${engine.id} stopped heartbeating · steps pinned to it will queue until it comes back rather than fail`,
        source: "registry",
      }));
    return [...fromValidate, ...offline];
  }, [validate.data, engines.data]);

  const engineStatuses: readonly EngineStatus[] = useMemo(
    () =>
      (engines.data?.engines ?? []).map((engine) => ({
        id: engine.id,
        label: engine.id,
        online: engine.state !== "gone",
        draining: engine.state === "draining",
      })),
    [engines.data],
  );

  const onCommand = useCallback(
    (menuId: string, itemId: string) => {
      if (itemId === "theme") toggleTheme();
      else if (itemId === "diagnostics") setDiagnosticsOpen((open) => !open);
      else setNotice(`${menuId}: ${itemId} is not wired up yet`);
    },
    [toggleTheme],
  );

  const actions: readonly ToolbarAction[] = [
    { id: "validate", label: "validate", busy: validate.isPending },
    { id: "plan", label: "plan · dry-run", busy: plan.isPending },
    { id: "run", label: "run", primary: true, busy: start.isPending },
  ];

  const onAction = useCallback(
    (id: string) => {
      setNotice("");
      if (id === "validate") validate.mutate();
      else if (id === "plan") plan.mutate();
      else if (id === "run") {
        start.mutate(undefined, {
          onSuccess: (response) => {
            // A started run is a different thing to look at, and the run view
            // is addressed by hash so the link survives being pasted.
            globalThis.location.hash = `#/runs/${response.runId}`;
          },
        });
      }
    },
    [validate, plan, start],
  );

  const errors = diagnostics.filter((d) => d.severity === "error").length;
  const status =
    notice !== ""
      ? notice
      : pipeline.isError
        ? "could not load this revision"
        : validate.isError
          ? "validate failed — the plane refused the request"
          : `${errors} error${errors === 1 ? "" : "s"} · rev ${revisionId.slice(0, 11)}`;

  return (
    <Shell
      menuBar={
        <MenuBar
          menus={menus}
          onCommand={onCommand}
          tenant={tenant}
          pipelineName={pipeline.data?.pipeline?.id ?? pipelineId}
          revisionId={revisionId}
          revisionState={
            revision === undefined
              ? "unknown"
              : revision.state === "active"
                ? "active"
                : "draft"
          }
          themeLabel={theme === "dark" ? "☾ dark" : "☀ light"}
          onToggleTheme={toggleTheme}
          user={user}
        />
      }
      toolbar={
        <Toolbar
          actions={actions}
          onAction={onAction}
          status={status}
          agentEditsPending={0}
          onOpenAgentEdits={() => setNotice("no agent edits are pending")}
          onOpenAssistant={() =>
            setNotice("the assistant has no endpoint in this build")
          }
        />
      }
      sidebar={
        <Sidebar
          catalog={[]}
          catalogIsSample={false}
          categories={categories}
          activeCategory={category}
          onCategory={setCategory}
          query={query}
          onQuery={setQuery}
          onAdd={() =>
            setNotice("adding from the catalog needs a plugin list RPC")
          }
          onBrowseRegistry={() =>
            setNotice("the plugin registry has no endpoint in this build")
          }
          runs={[]}
          runsAreSample={false}
          onOpenRun={(runId) => {
            globalThis.location.hash = `#/runs/${runId}`;
          }}
        />
      }
      canvas={
        <Canvas
          pipelineId={pipelineId}
          revisionId={revisionId}
          variant="embedded"
          onSelect={setSelected}
          theme={theme}
        />
      }
      inspector={
        <Inspector
          tab={tab}
          onTab={setTab}
          step={
            selectedStep === null
              ? null
              : {
                  id: selectedStep.id,
                  name:
                    selectedStep.name === ""
                      ? selectedStep.id
                      : selectedStep.name,
                  pluginRef: selectedStep.pluginRef,
                  effect: effectOf(selectedStep.effectClass),
                  engine: selectedStep.engineType,
                  produces: asPortTypeName(
                    selectedStep.outputs[0]?.type?.kind.case === "blob"
                      ? "blob"
                      : selectedStep.outputs[0]?.type?.kind.case ===
                          "structured"
                        ? selectedStep.outputs[0].type.kind.value.schemaId
                        : undefined,
                  ),
                }
          }
          onEffect={() =>
            setNotice(
              "changing an effect class is an operation — not wired to ApplyOperation yet",
            )
          }
        />
      }
      diagnostics={
        <Diagnostics
          diagnostics={diagnostics}
          open={diagnosticsOpen}
          onToggle={() => setDiagnosticsOpen((open) => !open)}
          onSelectStep={setSelected}
        />
      }
      statusBar={
        <StatusBar
          engines={engineStatuses}
          busConnected={!engines.isError}
          runsToday={null}
          tenant={tenant}
        />
      }
    />
  );
}
