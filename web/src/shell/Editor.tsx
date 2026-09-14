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
import { useCallback, useEffect, useMemo, useState } from "react";

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
import {
  ApprovalGate,
  AssistantPanel,
  decideGate,
  type GateDecision,
} from "./assistant/index.js";
import {
  CommandPalette,
  EnginesModal,
  GitModal,
  ImportModal,
  RegistryModal,
  SettingsModal,
  Toasts,
  type Command,
  type FleetEngine,
  type Toast,
  type ToastTone,
} from "./panels/index.js";
import { asPortTypeName } from "../design/PortGlyph.js";
import { statusOf } from "../canvas/stepStatus.js";
import { useRunModel } from "../run/useRunModel.js";
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
  runId = null,
}: {
  readonly pipelineId: string;
  readonly revisionId: string;
  readonly tenant: string;
  readonly user: string;
  /** A run to follow while editing. The canvas paints its progress on the same
   * nodes rather than sending the user to a different screen: "which step is
   * this stuck on" is a question about the graph they are already looking at. */
  readonly runId?: string | null;
}) {
  const { theme, toggleTheme } = useTheme();
  const [tab, setTab] = useState<"properties" | "logs">("properties");
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(true);
  const [selected, setSelected] = useState<string | null>(null);
  const [category, setCategory] = useState<string>("all");
  const [query, setQuery] = useState("");
  const [notice, setNotice] = useState("");
  // A dry run's answer, held until the definition changes under it. Clearing
  // it on every edit is the point: a plan describes ONE revision, and a badge
  // left over from the previous one is a confident wrong answer.
  const [assistantOpen, setAssistantOpen] = useState(false);
  const [assistantInput, setAssistantInput] = useState("");
  const [gateFor, setGateFor] = useState<string | null>(null);
  // Which modal is open, by the menu id that opens it. One at a time: two
  // stacked dialogs is two Escape presses to get back to the canvas.
  const [modal, setModal] = useState<
    "settings" | "engines" | "registry" | "git" | "import" | null
  >(null);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [toasts, setToasts] = useState<readonly Toast[]>([]);
  const [planned, setPlanned] = useState<ReadonlyMap<
    string,
    { cacheHit: boolean; engineKind: string }
  > | null>(null);

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
    onSuccess: (response) => {
      setPlanned(
        new Map(
          response.steps.map((step) => [
            step.stepId,
            { cacheHit: step.cacheHit, engineKind: step.engineKind },
          ]),
        ),
      );
      const hits = response.steps.filter((step) => step.cacheHit).length;
      setNotice(
        `plan: ${response.steps.length - hits} would run · ${hits} from cache`,
      );
    },
  });

  const start = useMutation({
    mutationFn: () => pipelineClient.startRun({ pipelineId, revisionId }),
  });

  const run = useRunModel(runId);
  const runStatus = useMemo(
    () => new Map(run.nodes.map((node) => [node.id, statusOf(node)] as const)),
    [run],
  );

  const revision = pipeline.data?.revision;
  const steps = useMemo(
    () => pipeline.data?.pipeline?.steps ?? [],
    [pipeline.data],
  );

  const selectedStep = useMemo(
    () => steps.find((step) => step.id === selected) ?? null,
    [steps, selected],
  );

  // A gate the plane is waiting on. The editor finds it in the run rather than
  // being told: a step that is blocked IS the approval request, and a separate
  // notification would be a second source of truth for the same fact.
  const blocked = run.nodes.find((node) => statusOf(node) === "blocked");

  const decide = useMutation({
    mutationFn: (decision: GateDecision) =>
      decideGate(pipelineClient, run.runId, gateFor ?? "", decision),
    onSuccess: (_response, { approved }) => {
      setGateFor(null);
      setNotice(approved ? "gate approved" : "gate denied");
    },
  });

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
    const gates: Diagnostic[] =
      blocked === undefined
        ? []
        : [
            {
              id: `gate-${blocked.id}`,
              severity: "warning" as const,
              text: `${blocked.id} is waiting for a decision — click to open the gate`,
              source: "approval",
              stepId: blocked.id,
            },
          ];
    return [...gates, ...fromValidate, ...offline];
  }, [validate.data, engines.data, blocked]);

  const fleet: readonly FleetEngine[] = useMemo(
    () =>
      (engines.data?.engines ?? []).map((engine) => ({
        id: engine.id,
        state:
          engine.state === "ready" ||
          engine.state === "draining" ||
          engine.state === "registering"
            ? engine.state
            : "offline",
        capabilities: engine.capabilities.map((capability) =>
          String(capability),
        ),
        os: engine.os,
        arch: engine.arch,
        slots: engine.slots,
        protocolVersions: engine.protocolVersions,
        inFlight: engine.inFlight.length,
      })),
    [engines.data],
  );

  const drain = useMutation({
    mutationFn: (engineId: string) => engineClient.drainEngine({ engineId }),
    onSuccess: () => say("drain requested — the engine finishes what it holds"),
  });

  const busUrl = globalThis.location?.origin ?? "";

  const commands: readonly Command[] = useMemo(
    () => [
      { id: "validate", label: "validate this revision" },
      { id: "plan", label: "plan · dry-run" },
      { id: "run", label: "start a run" },
      { id: "theme", label: "toggle theme" },
      { id: "engines", label: "engines…" },
      { id: "registry", label: "browse plugin registry…" },
      { id: "settings", label: "settings…" },
    ],
    [],
  );

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

  const say = useCallback((text: string, tone: ToastTone = "info") => {
    const id = `${Date.now()}-${text}`;
    setToasts((current) => [...current, { id, text, tone }]);
  }, []);

  const onCommand = useCallback(
    (_menuId: string, itemId: string) => {
      if (itemId === "theme") toggleTheme();
      else if (itemId === "diagnostics") setDiagnosticsOpen((open) => !open);
      else if (
        itemId === "settings" ||
        itemId === "engines" ||
        itemId === "registry" ||
        itemId === "git" ||
        itemId === "import"
      ) {
        setModal(itemId);
      } else if (itemId === "palette") setPaletteOpen(true);
      else say(`${itemId} is not wired up yet`, "warning");
    },
    [toggleTheme, say],
  );

  // ⌘K anywhere. It is on the window rather than on an input, because the
  // whole point of a palette is that you reach it without first clicking the
  // thing that would have focused it.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPaletteOpen((open) => !open);
      }
    };
    globalThis.addEventListener("keydown", onKey);
    return () => globalThis.removeEventListener("keydown", onKey);
  }, []);

  const actions: readonly ToolbarAction[] = [
    { id: "validate", label: "validate", busy: validate.isPending },
    {
      id: "plan",
      label: planned === null ? "plan · dry-run" : "plan · clear",
      busy: plan.isPending,
    },
    { id: "run", label: "run", primary: true, busy: start.isPending },
  ];

  const onAction = useCallback(
    (id: string) => {
      setNotice("");
      if (id === "validate") validate.mutate();
      else if (id === "plan") {
        // The same button clears it, because a dry run that cannot be
        // dismissed leaves the canvas showing predictions forever.
        if (planned === null) plan.mutate();
        else {
          setPlanned(null);
          setNotice("");
        }
      } else if (id === "run") {
        start.mutate(undefined, {
          onSuccess: (response) => {
            // A started run is a different thing to look at, and the run view
            // is addressed by hash so the link survives being pasted.
            globalThis.location.hash = `#/runs/${response.runId}`;
          },
        });
      }
    },
    [validate, plan, start, planned],
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
          status={
            runId === null ? status : `run ${runId.slice(0, 12)} · ${run.state}`
          }
          agentEditsPending={0}
          onOpenAgentEdits={() => setNotice("no agent edits are pending")}
          onOpenAssistant={() => setAssistantOpen((open) => !open)}
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
          onBrowseRegistry={() => setModal("registry")}
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
          {...(planned === null ? {} : { planned })}
          {...(runId === null ? {} : { runStatus })}
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
          onSelectStep={(stepId) => {
            setSelected(stepId);
            if (blocked?.id === stepId) setGateFor(stepId);
          }}
        />
      }
      overlays={
        <>
          <Toasts
            toasts={toasts}
            onDismiss={(id) =>
              setToasts((current) => current.filter((t) => t.id !== id))
            }
          />
          <CommandPalette
            open={paletteOpen}
            onClose={() => setPaletteOpen(false)}
            commands={commands}
            onRun={(id) => {
              setPaletteOpen(false);
              if (id === "theme") toggleTheme();
              else if (id === "validate" || id === "plan" || id === "run")
                onAction(id);
              else onCommand("palette", id);
            }}
          />
          {modal === "settings" && (
            <SettingsModal
              tenant={tenant}
              onClose={() => setModal(null)}
              onRevokeToken={() =>
                say("no token RPC in this contract", "warning")
              }
            />
          )}
          {modal === "engines" && (
            <EnginesModal
              engines={fleet}
              busUrl={busUrl}
              onDrain={(id) => drain.mutate(id)}
              onClose={() => setModal(null)}
            />
          )}
          {modal === "registry" && (
            <RegistryModal
              onClose={() => setModal(null)}
              onInstall={() => say("no registry endpoint", "warning")}
            />
          )}
          {modal === "git" && (
            <GitModal
              onClose={() => setModal(null)}
              onConnect={() => say("no git endpoint", "warning")}
            />
          )}
          {modal === "import" && (
            <ImportModal
              onClose={() => setModal(null)}
              onImport={() => say("no import endpoint", "warning")}
              onPickSource={() => undefined}
            />
          )}
          {assistantOpen && (
            <AssistantPanel
              messages={[]}
              busy={false}
              input={assistantInput}
              onInput={setAssistantInput}
              onSend={() => {
                setAssistantInput("");
                setNotice("no assistant endpoint — nothing was sent");
              }}
              onClose={() => setAssistantOpen(false)}
              onApplyOps={() =>
                setNotice("no assistant endpoint — nothing to apply")
              }
            />
          )}
          {gateFor !== null && (
            <ApprovalGate
              run={{
                id: run.runId,
                pipelineName: pipeline.data?.pipeline?.id ?? pipelineId,
                revisionId,
              }}
              step={{
                id: gateFor,
                name: gateFor,
                effect: "at-most-once",
              }}
              busy={decide.isPending}
              onDecide={(approved, reason) =>
                decide.mutate({ approved, reason })
              }
              onClose={() => setGateFor(null)}
            />
          )}
        </>
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
