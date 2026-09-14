/**
 * A step's capabilities and the secrets it is given, in the inspector.
 *
 * Every change is ONE operation naming ONE element — set_step_capability or
 * set_step_secret (ADR 0028) — and never the list. A list-shaped write would be
 * a document-level edit at the scale of one field: two people changing
 * different bindings on one step would overwrite each other, and the revision
 * history would say "secrets changed" instead of which one.
 *
 * The panel refuses, before sending, what the control plane would refuse
 * anyway: a variable name that is not one, and binding an env the step already
 * binds (which the plane would treat as a rebind the person did not ask for).
 * It does NOT refuse a secret without CAPABILITY_SECRETS — neither does the
 * plane, because refusing that would refuse the undo of the edit that fixes it
 * — and says so instead, with the one operation that repairs it.
 */
import { useState } from "react";

import type { Operation } from "../gen/dhole/v1/api_pb.js";
import { Capability, CapabilitySchema } from "../gen/dhole/v1/common_pb.js";
import type { Step } from "../gen/dhole/v1/pipeline_pb.js";
import { Label } from "./Inspector.js";

/** isEnvName is the scheduler's rule (internal/secrets.IsEnvName). */
export function isEnvName(value: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
}

/** The capabilities a step can declare, read off the GENERATED descriptor so
 * a capability added to the contract appears here without a web change. */
const capabilities = CapabilitySchema.values.filter(
  (value) => value.number !== Number(Capability.UNSPECIFIED),
);

export function secretOperation(
  stepId: string,
  env: string,
  name: string,
  remove = false,
): Operation {
  return {
    $typeName: "dhole.v1.Operation",
    kind: {
      case: "setStepSecret",
      value: { $typeName: "dhole.v1.SetStepSecret", stepId, env, name, remove },
    },
  };
}

export function capabilityOperation(
  stepId: string,
  capability: Capability,
  remove: boolean,
): Operation {
  return {
    $typeName: "dhole.v1.Operation",
    kind: {
      case: "setStepCapability",
      value: {
        $typeName: "dhole.v1.SetStepCapability",
        stepId,
        capability,
        remove,
      },
    },
  };
}

const box = {
  background: "var(--panel2)",
  border: "1px solid var(--line)",
  borderRadius: 5,
  padding: "6px 8px",
  fontSize: 10,
  color: "var(--ink2)",
} as const;

const input = {
  background: "transparent",
  border: "1px solid var(--line2)",
  borderRadius: 4,
  color: "var(--ink)",
  fontSize: 10,
  padding: "4px 6px",
  minWidth: 0,
  flex: 1,
} as const;

const button = {
  fontSize: 9,
  padding: "4px 8px",
  borderRadius: 4,
  border: "1px solid var(--line2)",
  background: "transparent",
  color: "var(--ink3)",
  cursor: "pointer",
} as const;

/** NameField holds a binding's name while it is being edited and rebinds on
 * Enter. Keyed by the name it started from, so a rebind that landed resets
 * it. */
function NameField({
  env,
  name,
  disabled,
  onRebind,
}: {
  readonly env: string;
  readonly name: string;
  readonly disabled: boolean;
  readonly onRebind: (name: string) => void;
}) {
  const [draft, setDraft] = useState(name);
  return (
    <input
      data-testid={`secret-name-${env}`}
      aria-label={`secret bound to ${env}`}
      value={draft}
      disabled={disabled}
      style={input}
      onChange={(event) => setDraft(event.target.value)}
      onKeyDown={(event) => {
        if (event.key === "Enter" && draft !== name && draft !== "") {
          onRebind(draft);
        }
      }}
    />
  );
}

export function StepDeclarations({
  step,
  busy = false,
  onOperation,
}: {
  readonly step: Step;
  readonly busy?: boolean;
  readonly onOperation: (operation: Operation) => void;
}) {
  const [env, setEnv] = useState("");
  const [name, setName] = useState("");
  const [refusal, setRefusal] = useState<string | null>(null);

  const declares = (capability: Capability) =>
    step.capabilities.includes(capability);
  const missingCapability =
    step.secrets.length > 0 && !declares(Capability.SECRETS);

  const bind = () => {
    if (!isEnvName(env)) {
      setRefusal(`"${env}" is not an environment variable name`);
      return;
    }
    if (step.secrets.some((secret) => secret.env === env)) {
      setRefusal(`${env} is already bound — change its secret above`);
      return;
    }
    if (name === "") {
      setRefusal(`name the secret ${env} should receive`);
      return;
    }
    setRefusal(null);
    onOperation(secretOperation(step.id, env, name));
    setEnv("");
    setName("");
  };

  return (
    <>
      <Label>CAPABILITIES</Label>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {capabilities.map((capability) => {
          const on = declares(capability.number);
          return (
            <button
              key={capability.name}
              type="button"
              data-testid={`capability-${capability.name}`}
              aria-pressed={on}
              disabled={busy}
              onClick={() =>
                onOperation(capabilityOperation(step.id, capability.number, on))
              }
              style={{
                ...button,
                border: `1px solid ${on ? "var(--accent)" : "var(--line2)"}`,
                background: on ? "var(--accent-soft)" : "transparent",
                color: on ? "var(--accent)" : "var(--ink3)",
              }}
            >
              {capability.name.replace(/^CAPABILITY_/, "").toLowerCase()}
            </button>
          );
        })}
      </div>

      <Label>SECRETS · by name, never by value</Label>
      {missingCapability && (
        <div
          data-testid="secrets-capability-missing"
          style={{
            fontSize: 9,
            color: "var(--warn)",
            lineHeight: 1.6,
            marginBottom: 8,
          }}
        >
          this step binds secrets but does not declare CAPABILITY_SECRETS, so no
          engine will be given it and the scheduler refuses it.{" "}
          <button
            type="button"
            data-testid="secrets-capability-declare"
            disabled={busy}
            style={button}
            onClick={() =>
              onOperation(
                capabilityOperation(step.id, Capability.SECRETS, false),
              )
            }
          >
            declare it
          </button>
        </div>
      )}
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {step.secrets.map((secret) => (
          <div
            key={secret.env}
            data-testid={`secret-${secret.env}`}
            style={{ ...box, display: "flex", alignItems: "center", gap: 6 }}
          >
            <span style={{ color: "var(--ink)", whiteSpace: "nowrap" }}>
              {secret.env}
            </span>
            <span style={{ color: "var(--ink3)" }}>←</span>
            <NameField
              key={secret.name}
              env={secret.env}
              name={secret.name}
              disabled={busy}
              onRebind={(next) =>
                onOperation(secretOperation(step.id, secret.env, next))
              }
            />
            <button
              type="button"
              data-testid={`secret-remove-${secret.env}`}
              aria-label={`unbind ${secret.env}`}
              disabled={busy}
              style={button}
              onClick={() =>
                onOperation(secretOperation(step.id, secret.env, "", true))
              }
            >
              ×
            </button>
          </div>
        ))}
        <div style={{ display: "flex", gap: 6 }}>
          <input
            data-testid="secret-new-env"
            aria-label="environment variable"
            placeholder="ENV_VAR"
            value={env}
            disabled={busy}
            style={input}
            onChange={(event) => setEnv(event.target.value)}
          />
          <input
            data-testid="secret-new-name"
            aria-label="secret name"
            placeholder="secret-name"
            value={name}
            disabled={busy}
            style={input}
            onChange={(event) => setName(event.target.value)}
          />
          <button
            type="button"
            data-testid="secret-bind"
            disabled={busy}
            style={button}
            onClick={bind}
          >
            bind
          </button>
        </div>
        {refusal !== null && (
          <div role="alert" style={{ fontSize: 9, color: "var(--err)" }}>
            {refusal}
          </div>
        )}
      </div>
    </>
  );
}
