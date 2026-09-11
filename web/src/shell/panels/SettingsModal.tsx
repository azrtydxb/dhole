/**
 * Tenant settings: the four things about a tenant that change what a run is
 * allowed to do.
 *
 * They are TABS down a rail rather than one long scroll because they are read
 * at different moments and by different people — identity and retention on the
 * day the tenant is set up, members and tokens whenever somebody joins or a
 * credential leaks, policy tiers when a deploy is refused and nobody can see
 * why. A single scroll makes the last of those, which is the urgent one, the
 * furthest away.
 *
 * Policy tiers get the CEL rule printed verbatim and selectable. A denial that
 * says only "policy" sends somebody to read the server's source; the rule that
 * refused them, on screen and copyable, is the difference between a two-minute
 * fix and a ticket.
 *
 * There is no SettingsService. Every value below is a fixture, which is what
 * the sample chip in the title row is for — without it this panel reads as the
 * tenant's real configuration and somebody will plan around a retention window
 * nothing enforces.
 */
import { useState } from "react";

import { Modal } from "./Modal.js";

/** SettingsTab is the rail, in the order it is drawn. */
export type SettingsTab = "general" | "members" | "tokens" | "policy";

const tabs: readonly { id: SettingsTab; label: string }[] = [
  { id: "general", label: "general" },
  { id: "members", label: "members & roles" },
  { id: "tokens", label: "service tokens" },
  { id: "policy", label: "policy tiers" },
];

function Section({
  heading,
  children,
}: {
  readonly heading: string;
  readonly children: React.ReactNode;
}) {
  return (
    <div>
      <div
        style={{
          color: "var(--ink3)",
          fontSize: 9,
          letterSpacing: "0.1em",
          marginBottom: 5,
        }}
      >
        {heading}
      </div>
      <div style={{ color: "var(--ink2)", lineHeight: 1.7 }}>{children}</div>
    </div>
  );
}

const rowStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 10,
  border: "1px solid var(--line2)",
  borderRadius: 6,
  padding: "8px 11px",
};

function Avatar({
  initials,
  accent,
}: {
  readonly initials: string;
  readonly accent: "owner" | "member" | "agent";
}) {
  return (
    <span
      aria-hidden
      style={{
        width: 20,
        height: 20,
        borderRadius: "50%",
        background:
          accent === "agent"
            ? "var(--accent)"
            : accent === "owner"
              ? "var(--accent-soft)"
              : "var(--line2)",
        border: accent === "owner" ? "1px solid var(--accent)" : "none",
        color:
          accent === "agent"
            ? "#fff"
            : accent === "owner"
              ? "var(--accent)"
              : "var(--ink2)",
        fontSize: 8,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        flex: "none",
      }}
    >
      {initials}
    </span>
  );
}

function RoleChip({
  label,
  tone,
}: {
  readonly label: string;
  readonly tone: "owner" | "editable" | "grant";
}) {
  const colour =
    tone === "owner"
      ? "var(--rust)"
      : tone === "grant"
        ? "var(--accent)"
        : "var(--ink2)";
  return (
    <span
      style={{
        marginLeft: "auto",
        border: `1px solid ${tone === "editable" ? "var(--line)" : colour}`,
        color: colour,
        borderRadius: 3,
        padding: "1px 7px",
        fontSize: 9,
        flex: "none",
      }}
    >
      {label}
    </span>
  );
}

function Note({ children }: { readonly children: React.ReactNode }) {
  return (
    <div style={{ color: "var(--ink3)", lineHeight: 1.6, marginTop: 6 }}>
      {children}
    </div>
  );
}

function General() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <Section heading="IDENTITY">
        auth: OIDC · idp.acme.dev ✓ connected
        <br />
        fallback: built-in local users (2)
      </Section>
      <Section heading="RETENTION">
        run event log: 365d (audit trail)
        <br />
        authoritative logs &amp; artifacts: 30d
        <br />
        CAS blobs: refcounted from retained runs
        <br />
        LLM call records: 90d (holds prompt content)
      </Section>
      <Section heading="DEPLOYMENT">
        mode: clustered · Postgres + NATS (3 nodes)
        <br />
        object storage: s3://acme-dhole ✓
        <br />
        single-binary mode available: embedded NATS + SQLite + in-process engine
      </Section>
      <Section heading="SCHEDULER">
        weighted fair queuing · dispatch target &lt;1s at load
        <br />
        tenant concurrency budget: 64 steps
        <br />
        per-pipeline budget: 8 · trigger overlap: queue (budget 1 = skip)
      </Section>
      <Section heading="PLUGIN RESOLUTION">
        schemes: oci:// (ghcr.io mirror) · cas:// (local store)
        <br />
        upstreams: 2 federated, locally mirrored · signatures: per trust tier
      </Section>
    </div>
  );
}

function Members() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <div style={rowStyle}>
        <Avatar initials="rw" accent="owner" />
        <span style={{ color: "var(--ink)" }}>rw@acme.dev</span>
        <RoleChip label="owner" tone="owner" />
      </div>
      <div style={rowStyle}>
        <Avatar initials="mk" accent="member" />
        <span style={{ color: "var(--ink)" }}>maya@acme.dev</span>
        <RoleChip label="editor ▾" tone="editable" />
      </div>
      <div style={rowStyle}>
        <Avatar initials="op" accent="member" />
        <span style={{ color: "var(--ink)" }}>ops@acme.dev</span>
        <RoleChip label="operator ▾" tone="editable" />
      </div>
      <div
        style={{
          ...rowStyle,
          border: "1px solid var(--accent)",
          background: "var(--accent-soft)",
        }}
      >
        <Avatar initials="ai" accent="agent" />
        <span style={{ color: "var(--ink)" }}>agent:claude</span>
        <span style={{ color: "var(--ink3)", fontSize: 9 }}>service token</span>
        <RoleChip label="pipeline.edit · run.read" tone="grant" />
      </div>
      <Note>
        roles: owner · admin · editor · operator · viewer. Agents hold scoped
        tokens, never roles — same API, narrower grants.
      </Note>
    </div>
  );
}

/** The token rows are a fixture. `onRevoke` is still a real callback so the
 * day a token RPC lands the only change here is where the list comes from. */
function Tokens({ onRevoke }: { readonly onRevoke: (id: string) => void }) {
  const rows: readonly { id: string; scope: string }[] = [
    {
      id: "dh_svc_91kd…",
      scope: "agent:claude · pipeline.edit, run.read · 30d",
    },
    { id: "dh_eng_7f3a…", scope: "engine join · tier default · no expiry" },
    { id: "dh_ci_44xn…", scope: "cli · full parity · 90d" },
  ];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {rows.map((row) => (
        <div key={row.id} style={rowStyle}>
          <span style={{ color: "var(--ink)", flex: "none" }}>{row.id}</span>
          <span style={{ color: "var(--ink3)" }}>{row.scope}</span>
          <button
            type="button"
            onClick={() => {
              onRevoke(row.id);
            }}
            style={{
              marginLeft: "auto",
              background: "none",
              border: "1px solid var(--err)",
              borderRadius: 3,
              color: "var(--err)",
              fontSize: 9,
              padding: "1px 8px",
              cursor: "pointer",
              flex: "none",
            }}
          >
            revoke
          </button>
        </div>
      ))}
      <Note>
        tokens keep working when the IdP is down — interactive login fails
        loudly instead of falling back.
      </Note>
    </div>
  );
}

function Policy() {
  const tiers: readonly { name: string; colour: string; rest: string }[] = [
    {
      name: "trusted",
      colour: "var(--ok)",
      rest: " — all capabilities · at-most-once allowed · privileged engines",
    },
    {
      name: "default",
      colour: "var(--warn)",
      rest: " — at-most-once requires approval gate · signed plugins only",
    },
    {
      name: "untrusted",
      colour: "var(--err)",
      rest: " — sandboxed engines only · no secrets · tainted data gated",
    },
  ];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {tiers.map((tier) => (
        <div
          key={tier.name}
          style={{
            border: "1px solid var(--line2)",
            borderRadius: 6,
            padding: "8px 11px",
            lineHeight: 1.7,
          }}
        >
          <span style={{ color: tier.colour, fontWeight: 700 }}>
            {tier.name}
          </span>
          <span style={{ color: "var(--ink3)" }}>{tier.rest}</span>
        </div>
      ))}
      <div
        className="dh-selectable"
        style={{
          background: "var(--panel2)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          padding: "9px 11px",
          marginTop: 4,
          fontSize: 9,
          lineHeight: 1.7,
          color: "var(--ink2)",
        }}
      >
        // CEL · evaluated at one point · &lt;10ms cached
        <br />
        step.effect == &quot;at_most_once&quot;
        <br />
        &nbsp;&nbsp;? tier == &quot;trusted&quot; || has(step.approval_gate)
        <br />
        &nbsp;&nbsp;: true
      </div>
      <div style={{ color: "var(--ink3)", lineHeight: 1.6, marginTop: 2 }}>
        denials fail closed and are audited with rule, tier and subject.
      </div>
    </div>
  );
}

export function SettingsModal({
  tenant,
  onClose,
  onRevokeToken,
  initialTab = "general",
  sample = true,
}: {
  readonly tenant: string;
  readonly onClose: () => void;
  readonly onRevokeToken: (tokenId: string) => void;
  /** Which pane to land on. "members & roles…" in the admin menu opens the
   * same dialog on a different tab rather than a second one. */
  readonly initialTab?: SettingsTab;
  readonly sample?: boolean;
}) {
  const [tab, setTab] = useState<SettingsTab>(initialTab);

  return (
    <Modal
      title={`settings · tenant ${tenant}`}
      width={620}
      height={400}
      sample={sample}
      onClose={onClose}
    >
      <div style={{ flex: 1, display: "flex", minHeight: 0 }}>
        <div
          role="tablist"
          aria-label="settings sections"
          style={{
            width: 150,
            borderRight: "1px solid var(--line2)",
            padding: 10,
            display: "flex",
            flexDirection: "column",
            gap: 2,
            flex: "none",
          }}
        >
          {tabs.map((entry) => {
            const active = entry.id === tab;
            return (
              <button
                key={entry.id}
                type="button"
                role="tab"
                aria-selected={active}
                onClick={() => {
                  setTab(entry.id);
                }}
                style={{
                  padding: "6px 9px",
                  fontSize: 10,
                  borderRadius: 4,
                  border: "none",
                  cursor: "pointer",
                  textAlign: "left",
                  color: active ? "var(--ink)" : "var(--ink3)",
                  background: active ? "var(--accent-soft)" : "none",
                }}
              >
                {entry.label}
              </button>
            );
          })}
        </div>
        <div
          role="tabpanel"
          style={{
            flex: 1,
            overflowY: "auto",
            padding: 16,
            fontSize: 10,
            minWidth: 0,
          }}
        >
          {tab === "general" && <General />}
          {tab === "members" && <Members />}
          {tab === "tokens" && <Tokens onRevoke={onRevokeToken} />}
          {tab === "policy" && <Policy />}
        </div>
      </div>
    </Modal>
  );
}
