/**
 * One step's properties, as the plugin declares them.
 *
 * The panel holds no idea of what a step takes. It reads the plugin's input
 * schema off the definition the control plane returned and renders THAT
 * (ADR 0012, ADR 0013): publishing a plugin with a new field must put the
 * field on this screen without a web release, or the schema has stopped being
 * the truth and the editor has become a second declaration of every plugin.
 *
 * WHERE THE SCHEMA COMES FROM, AND WHY IT IS NOT AN RPC. The contract has no
 * catalog RPC yet — PipelineService serves pipelines, runs and validation, and
 * internal/catalog is reachable only from inside the control plane. What does
 * reach the browser is the declaration the definition carries: a step's
 * structured input port carries its schema inline (`StructType.schema`,
 * pipeline.proto), which is the plugin's InputSchema as it arrived in the
 * document. That is what this panel renders. The day the contract grows a
 * catalog RPC, only `declarationOf` below changes.
 *
 * WHAT CAN BE SAVED IS NARROWER THAN WHAT IS SHOWN, AND SAYS SO. The editing
 * vocabulary is closed and small (ADR 0020): SetProperty covers plugin_ref,
 * effect_class and lease_scope, and `Step` has nowhere to store a plugin's
 * own input values. A declared field this panel cannot persist is therefore
 * rendered read-only with that reason attached. It is NOT hidden: a field
 * dropped because the editor cannot save it is a value the user can neither
 * set nor see is missing, and the run would use the plugin's default without
 * anyone having chosen it.
 */
import { clone, create, type DescEnum } from "@bufbuild/protobuf";
import { useQuery } from "@tanstack/react-query";
import { useCallback, useMemo, useState } from "react";

import { pipelineClient } from "../api/client.js";
import {
  ChangeKind,
  ChangeSchema,
  type Change,
  DiffSchema,
  type Diagnostic,
  type Diff,
  type Operation,
} from "../gen/dhole/v1/api_pb.js";
import {
  EffectClassSchema,
  LeaseScopeSchema,
} from "../gen/dhole/v1/common_pb.js";
import {
  PipelineSchema,
  type Pipeline,
  type Step,
} from "../gen/dhole/v1/pipeline_pb.js";
import { DiffView } from "../review/DiffView.js";
import {
  fieldsOf,
  parseSchema,
  SchemaField,
  validateValues,
  type Field,
  type FormValues,
  type SchemaObject,
} from "./schemaForm.js";

/** PropertyPanelProps names the step being edited.
 *
 * The pipeline and the revision are required beside the step id: every edit
 * is applied against a base revision (ADR 0013), and a panel that guessed one
 * would be guessing whose work it is about to overwrite.
 */
export interface PropertyPanelProps {
  readonly pipelineId: string;
  readonly revisionId: string;
  readonly stepId: string;
}

/** The step properties SetProperty accepts, named as the contract names them
 * (api.proto, internal/api/operations.go settableProperties). */
const pluginRefProperty = "plugin_ref";
const effectClassProperty = "effect_class";
const leaseScopeProperty = "lease_scope";
const settable = [pluginRefProperty, effectClassProperty, leaseScopeProperty];

/** Why a declared field is on screen but not editable here. */
const notStorable =
  "no operation in the contract stores this value yet — SetProperty covers " +
  `${settable.join(", ")} — so it is shown as the plugin declares it.`;

/** enumNames reads an enum's declared value names off the GENERATED
 * descriptor. Typing the list here would be a second copy of the wire
 * contract, free to drift from it. */
function enumNames(desc: DescEnum): readonly string[] {
  return desc.values.map((value) => value.name);
}

function enumNumber(desc: DescEnum, name: string): number | undefined {
  return desc.values.find((value) => value.name === name)?.number;
}

function enumName(desc: DescEnum, number: number): string {
  return desc.values.find((value) => value.number === number)?.name ?? "";
}

/** declarationOf finds the plugin's input schema on the step.
 *
 * A step may declare several structured inputs; the first that carries a
 * schema is the one rendered, and any others are named in the note rather
 * than passed over in silence.
 */
function declarationOf(step: Step | undefined): {
  readonly source: string;
  readonly port: string;
  readonly others: readonly string[];
} {
  const carrying = (step?.inputs ?? []).filter((port) => {
    const kind = port.type?.kind;
    return kind?.case === "structured" && kind.value.schema !== "";
  });
  const first = carrying[0];
  const kind = first?.type?.kind;
  if (first === undefined || kind?.case !== "structured") {
    return { source: "", port: "", others: [] };
  }
  return {
    source: kind.value.schema,
    port: first.name,
    others: carrying.slice(1).map((port) => port.name),
  };
}

/** stepFields are the three properties the contract can set, drawn from the
 * plugin's declaration where it says anything about them and from the
 * generated descriptor otherwise. */
function stepFields(declared: readonly Field[]): readonly Field[] {
  const declaredBy = new Map(declared.map((field) => [field.name, field]));
  const enumField = (name: string, desc: DescEnum): Field => {
    const from = declaredBy.get(name);
    return {
      name,
      title: from?.title ?? name,
      description: from?.description ?? "",
      required: from?.required ?? false,
      fallback: from?.fallback ?? "",
      // The values come from the contract's own enum, not from the plugin:
      // set_property refuses anything else, so offering another list would
      // offer a choice the API will reject.
      control: { kind: "select", options: enumNames(desc) },
    };
  };
  const ref = declaredBy.get(pluginRefProperty);
  return [
    ref ?? {
      name: pluginRefProperty,
      title: pluginRefProperty,
      description: "",
      required: false,
      fallback: "",
      control: { kind: "text" },
    },
    enumField(effectClassProperty, EffectClassSchema),
    enumField(leaseScopeProperty, LeaseScopeSchema),
  ];
}

/** pending is one proposed edit, in the contract's own vocabulary. */
interface Pending {
  readonly property: string;
  readonly from: string;
  readonly to: string;
}

/** review is what the person is being asked to confirm. */
interface Review {
  readonly pending: readonly Pending[];
  readonly diff: Diff;
  readonly diagnostics: readonly Diagnostic[];
}

/** applied is what the control plane did. */
interface Applied {
  readonly revisionId: string;
  readonly diff: Diff;
  readonly diagnostics: readonly Diagnostic[];
}

/** PropertyPanel renders one step's declaration and edits it through the
 * contract's operations. */
export function PropertyPanel({
  pipelineId,
  revisionId,
  stepId,
}: PropertyPanelProps) {
  const [head, setHead] = useState(revisionId);
  const [edited, setEdited] = useState<Pipeline | undefined>(undefined);
  const [edits, setEdits] = useState<
    Readonly<Record<string, string | boolean>>
  >({});
  const [attempted, setAttempted] = useState(false);
  const [errors, setErrors] = useState<Readonly<Record<string, string>>>({});
  const [review, setReview] = useState<Review | undefined>(undefined);
  const [applied, setApplied] = useState<Applied | undefined>(undefined);
  const [failure, setFailure] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const loaded = useQuery({
    queryKey: ["pipeline", pipelineId, revisionId],
    queryFn: () => pipelineClient.getPipeline({ pipelineId, revisionId }),
  });

  const pipeline = edited ?? loaded.data?.pipeline;
  const step = pipeline?.steps.find((candidate) => candidate.id === stepId);

  const declaration = useMemo(() => declarationOf(step), [step]);

  const parsed = useMemo(
    () =>
      declaration.source === "" ? undefined : parseSchema(declaration.source),
    [declaration.source],
  );
  const schema: SchemaObject | undefined =
    parsed !== undefined && "schema" in parsed ? parsed.schema : undefined;
  const rendered = useMemo(
    () => (schema === undefined ? undefined : fieldsOf(schema)),
    [schema],
  );
  const declaredFields: readonly Field[] = useMemo(
    () =>
      rendered !== undefined && "fields" in rendered ? rendered.fields : [],
    [rendered],
  );

  // Why the form is not the whole declaration, when it is not. Every branch
  // here ends in a sentence on screen: a declaration that cannot be rendered
  // must never look like a plugin that declares nothing.
  const schemaNote =
    declaration.source === ""
      ? "this step declares no plugin input schema, so only the properties the " +
        "contract itself defines are shown."
      : parsed !== undefined && "error" in parsed
        ? parsed.error
        : rendered !== undefined && "error" in rendered
          ? rendered.error
          : declaration.others.length > 0
            ? `rendering the schema on port "${declaration.port}"; ports ` +
              `${declaration.others.join(", ")} also declare one and are not shown.`
            : "";

  const fields = useMemo(() => {
    const contract = stepFields(declaredFields);
    const rest = declaredFields.filter(
      (field) => !settable.includes(field.name),
    );
    return [...contract, ...rest];
  }, [declaredFields]);

  const base: FormValues = useMemo(() => {
    const values: Record<string, string | number | boolean> = {};
    for (const field of fields) {
      values[field.name] = field.fallback;
    }
    values[pluginRefProperty] = step?.pluginRef ?? "";
    values[effectClassProperty] = enumName(
      EffectClassSchema,
      step?.effectClass ?? 0,
    );
    values[leaseScopeProperty] = enumName(
      LeaseScopeSchema,
      step?.leaseScope ?? 0,
    );
    return values;
  }, [fields, step]);

  const values: FormValues = useMemo(
    () => ({ ...base, ...edits }),
    [base, edits],
  );

  const change = useCallback((name: string, value: string | boolean) => {
    setEdits((held) => ({ ...held, [name]: value }));
    setReview(undefined);
    setApplied(undefined);
  }, []);

  /** pendingEdits is the difference between the form and the step, in the
   * contract's terms. Nothing else is savable. */
  const pendingEdits = useCallback((): readonly Pending[] => {
    const out: Pending[] = [];
    for (const property of settable) {
      const to = String(values[property] ?? "");
      const from = String(base[property] ?? "");
      if (to !== from) {
        out.push({ property, from, to });
      }
    }
    return out;
  }, [base, values]);

  /** proposed is the definition as it would stand, so the control plane can
   * be asked about the RESULT rather than about what is saved now. */
  const proposed = useCallback(
    (pending: readonly Pending[]): Pipeline | undefined => {
      if (pipeline === undefined) {
        return undefined;
      }
      const next = clone(PipelineSchema, pipeline);
      const target = next.steps.find((candidate) => candidate.id === stepId);
      if (target === undefined) {
        return undefined;
      }
      for (const edit of pending) {
        switch (edit.property) {
          case pluginRefProperty:
            target.pluginRef = edit.to;
            break;
          case effectClassProperty:
            target.effectClass =
              enumNumber(EffectClassSchema, edit.to) ?? target.effectClass;
            break;
          case leaseScopeProperty:
            target.leaseScope =
              enumNumber(LeaseScopeSchema, edit.to) ?? target.leaseScope;
            break;
          default:
            break;
        }
      }
      return next;
    },
    [pipeline, stepId],
  );

  const save = useCallback(async () => {
    setAttempted(true);
    setFailure(null);
    setApplied(undefined);

    // The schema decides, here, before anything is sent. A panel that showed
    // this and sent the operation anyway would look identical on screen.
    const problems =
      schema === undefined ? {} : validateValues(schema, fields, values);
    setErrors(problems);
    if (Object.keys(problems).length > 0) {
      setReview(undefined);
      return;
    }

    const pending = pendingEdits();
    const next = proposed(pending);
    if (pending.length === 0 || next === undefined) {
      setReview({
        pending: [],
        diff: create(DiffSchema, { changes: [] }),
        diagnostics: [],
      });
      return;
    }

    setBusy(true);
    try {
      // Validate is read-only and answers about a definition that has not
      // been saved, which is exactly what a review needs: the warnings shown
      // are about the pipeline the person is about to create.
      const answer = await pipelineClient.validate({
        pipelineId,
        pipeline: next,
      });
      setReview({
        pending,
        diff: create(DiffSchema, {
          changes: pending.map((edit) =>
            create(ChangeSchema, {
              kind: ChangeKind.CHANGED,
              stepId,
              summary: `set ${edit.property} of step "${stepId}" from "${edit.from}" to "${edit.to}"`,
            }),
          ),
        }),
        diagnostics: answer.diagnostics,
      });
    } catch (cause) {
      setFailure(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  }, [fields, pendingEdits, pipelineId, proposed, schema, stepId, values]);

  const confirm = useCallback(async () => {
    if (review === undefined || review.pending.length === 0) {
      setReview(undefined);
      return;
    }
    setBusy(true);
    let at = head;
    const changes: Change[] = [];
    try {
      for (const edit of review.pending) {
        const operation: Operation = {
          $typeName: "dhole.v1.Operation",
          kind: {
            case: "setProperty",
            value: {
              $typeName: "dhole.v1.SetProperty",
              stepId,
              property: edit.property,
              value: edit.to,
            },
          },
        };
        const response = await pipelineClient.applyOperation({
          pipelineId,
          baseRevision: at,
          operation,
        });
        // The server's diff, not this panel's: what was applied is what the
        // control plane says was applied.
        changes.push(...(response.diff?.changes ?? []));
        if (response.revision !== undefined) {
          at = response.revision.id;
        }
        if (response.pipeline !== undefined) {
          setEdited(response.pipeline);
        }
      }
      setHead(at);
      setEdits({});
      setAttempted(false);
      setReview(undefined);
      setApplied({
        revisionId: at,
        diff: create(DiffSchema, { changes }),
        diagnostics: review.diagnostics,
      });
    } catch (cause) {
      setFailure(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  }, [head, pipelineId, review, stepId]);

  if (loaded.isError) {
    return (
      <p role="alert" data-testid="panel-error">
        could not load {pipelineId}: {loaded.error.message}
      </p>
    );
  }
  if (loaded.isPending) {
    return <p>loading {pipelineId}…</p>;
  }
  if (step === undefined) {
    return (
      <p role="alert" data-testid="panel-error">
        revision {head} has no step {stepId}.
      </p>
    );
  }

  return (
    <section data-testid="property-panel">
      <h2>{stepId}</h2>
      <p>
        based on <code data-testid="panel-revision">{head}</code>
      </p>
      {schemaNote !== "" && (
        <p data-testid="schema-note" role="note" style={{ color: "#975a16" }}>
          {schemaNote}
        </p>
      )}

      {errors[""] !== undefined && (
        <p data-testid="form-error" role="alert" style={{ color: "#c53030" }}>
          {errors[""]}
        </p>
      )}

      <form
        onSubmit={(event) => {
          event.preventDefault();
          void save();
        }}
      >
        {fields.map((field) => {
          const storable = settable.includes(field.name);
          const readOnly =
            field.control.kind === "unsupported"
              ? "declared, and not editable here."
              : storable
                ? undefined
                : notStorable;
          const shown = attempted ? errors[field.name] : undefined;
          return (
            <SchemaField
              key={field.name}
              field={field}
              value={values[field.name]}
              {...(readOnly === undefined ? {} : { readOnlyBecause: readOnly })}
              {...(shown === undefined ? {} : { error: shown })}
              onChange={(value) => change(field.name, value)}
            />
          );
        })}

        <button data-testid="panel-save" type="submit" disabled={busy}>
          save
        </button>
      </form>

      {failure !== null && (
        <p data-testid="panel-error" role="alert" style={{ color: "#c53030" }}>
          {failure}
        </p>
      )}

      {review !== undefined && (
        <DiffView
          revisionId={head}
          diff={review.diff}
          diagnostics={review.diagnostics}
          applied={false}
          busy={busy}
          onConfirm={() => void confirm()}
          onCancel={() => setReview(undefined)}
        />
      )}

      {applied !== undefined && (
        <DiffView
          revisionId={applied.revisionId}
          diff={applied.diff}
          diagnostics={applied.diagnostics}
          applied
        />
      )}
    </section>
  );
}
