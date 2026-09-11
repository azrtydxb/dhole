/**
 * The plugin registry: what is installable, what is installed, and what each
 * one would be trusted to do.
 *
 * Signature state is on the row beside the name, not behind the install
 * button, because it decides whether the plugin can run at all below tier
 * trusted. Push it into a confirmation dialog and the user learns their
 * pipeline is unrunnable at dispatch time, three screens away from the
 * decision that caused it.
 *
 * The footer says what installing actually does — resolve, verify, mirror,
 * pin by digest — because "install" in a federated registry looks like it
 * fetches from an upstream at run time. It does not: a revision pins a digest,
 * so a moved tag upstream never changes what an existing revision runs, and an
 * operator who does not know that will chase a phantom "it changed by itself".
 *
 * The control plane has no ListPlugins. Everything below is a fixture and the
 * title row says so permanently — invented rows that read as a real federated
 * catalogue are exactly how somebody ends up writing a pipeline against a
 * plugin that does not exist.
 */
import { useState } from "react";

import { Modal } from "./Modal.js";

/** RegistryPlugin is one installable plugin as a browser row needs it. */
export type RegistryPlugin = {
  readonly key: string;
  readonly name: string;
  readonly version: string;
  readonly category: string;
  readonly upstream: string;
  /** Install count as the upstream reports it; "—" for a local mirror that
   * does not count. */
  readonly installs: string;
  readonly description: string;
  readonly signed: boolean;
  readonly installed: boolean;
  /** Designed for but out of this major version — offered, never installable,
   * so the interface it needs is visible before somebody assumes it is
   * missing by accident. */
  readonly unavailable?: boolean;
};

/** The design's browse list. Exported so a caller can see exactly what it is
 * passing, and replace it wholesale the day a registry RPC exists. */
export const sampleRegistryPlugins: readonly RegistryPlugin[] = [
  {
    key: "tfapply",
    name: "terraform-apply",
    version: "v2.3.1",
    category: "deploy",
    upstream: "hub.dhole.dev",
    installs: "4.1k",
    description: "plan + apply with state locking; at-most-once with fencing",
    signed: true,
    installed: false,
  },
  {
    key: "ghrelease",
    name: "gh-release",
    version: "v1.9.0",
    category: "deploy",
    upstream: "hub.dhole.dev",
    installs: "3.2k",
    description: "create a GitHub release with assets from the CAS",
    signed: true,
    installed: false,
  },
  {
    key: "cargo",
    name: "cargo-build",
    version: "v1.4.2",
    category: "build",
    upstream: "hub.dhole.dev",
    installs: "2.8k",
    description: "Rust build with declared target dir as typed output",
    signed: true,
    installed: false,
  },
  {
    key: "pytest",
    name: "pytest",
    version: "v3.0.4",
    category: "test",
    upstream: "hub.dhole.dev",
    installs: "5.6k",
    description: "test runner emitting a typed junit report",
    signed: true,
    installed: false,
  },
  {
    key: "ansible",
    name: "ansible-run",
    version: "v0.9.1",
    category: "deploy",
    upstream: "community.acme.dev",
    installs: "910",
    description: "playbook execution over ssh, idempotent by declaration",
    signed: true,
    installed: false,
  },
  {
    key: "mqtt",
    name: "mqtt-publish",
    version: "v1.1.0",
    category: "flow",
    upstream: "community.acme.dev",
    installs: "640",
    description: "publish a message to an MQTT broker",
    signed: true,
    installed: false,
  },
  {
    key: "discord",
    name: "notify-discord",
    version: "v2.0.2",
    category: "flow",
    upstream: "community.acme.dev",
    installs: "1.3k",
    description: "webhook notifications with templates",
    signed: true,
    installed: false,
  },
  {
    key: "whisper",
    name: "whisper-transcribe",
    version: "v0.5.0",
    category: "llm",
    upstream: "community.acme.dev",
    installs: "380",
    description: "GPU speech-to-text; requires engine capability gpu",
    signed: true,
    installed: false,
  },
  {
    key: "renovate",
    name: "renovate-deps",
    version: "v0.3.2",
    category: "scm",
    upstream: "community.acme.dev",
    installs: "210",
    description: "dependency update PRs — UNSIGNED, blocked below tier trusted",
    signed: false,
    installed: false,
  },
  {
    key: "fcvm",
    name: "firecracker-vm",
    version: "v0.1.0-alpha",
    category: "build",
    upstream: "community.acme.dev",
    installs: "95",
    description:
      "VM executor engine plugin — interface designed for, not in v1",
    signed: true,
    installed: false,
    unavailable: true,
  },
];

type RegistryTab = "browse" | "installed";

function installLabel(plugin: RegistryPlugin): {
  text: string;
  background: string;
  border: string;
  colour: string;
} {
  if (plugin.installed)
    return {
      text: "✓ active",
      background: "none",
      border: "var(--ok)",
      colour: "var(--ok)",
    };
  if (plugin.unavailable === true)
    return {
      text: "v2 only",
      background: "none",
      border: "var(--line)",
      colour: "var(--ink3)",
    };
  return {
    text: "install",
    background: "var(--accent)",
    border: "var(--accent)",
    colour: "#fff",
  };
}

export function RegistryModal({
  onClose,
  onInstall,
  plugins = sampleRegistryPlugins,
  sample = true,
}: {
  readonly onClose: () => void;
  /** Called with the plugin key. Nothing here changes its own list: what is
   * installed is the mirror's business, not this panel's. */
  readonly onInstall: (key: string) => void;
  readonly plugins?: readonly RegistryPlugin[];
  readonly sample?: boolean;
}) {
  const [tab, setTab] = useState<RegistryTab>("browse");
  const [query, setQuery] = useState("");

  const needle = query.trim().toLowerCase();
  const shown = plugins.filter(
    (plugin) =>
      (tab === "browse" || plugin.installed) &&
      (needle === "" || plugin.name.toLowerCase().includes(needle)),
  );
  const installedCount = plugins.filter((plugin) => plugin.installed).length;

  return (
    <Modal
      title="plugin registry"
      subtitle="federated · hub.dhole.dev + community.acme.dev · locally mirrored"
      width={640}
      height={440}
      sample={sample}
      onClose={onClose}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "10px 16px",
          borderBottom: "1px solid var(--line2)",
          flex: "none",
        }}
      >
        {(["browse", "installed"] as const).map((id) => {
          const active = id === tab;
          return (
            <button
              key={id}
              type="button"
              aria-pressed={active}
              onClick={() => {
                setTab(id);
              }}
              style={{
                fontSize: 10,
                padding: "4px 11px",
                borderRadius: 5,
                border: "none",
                cursor: "pointer",
                color: active ? "var(--accent)" : "var(--ink3)",
                background: active ? "var(--accent-soft)" : "none",
              }}
            >
              {id === "browse"
                ? "browse"
                : `installed · ${String(installedCount)}`}
            </button>
          );
        })}
        <input
          placeholder="search upstreams…"
          aria-label="search upstreams"
          value={query}
          onChange={(event) => {
            setQuery(event.currentTarget.value);
          }}
          style={{
            flex: 1,
            minWidth: 0,
            background: "var(--panel2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            color: "var(--ink)",
            fontSize: 10,
            padding: "6px 9px",
            outline: "none",
          }}
        />
      </div>

      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "12px 16px",
          display: "flex",
          flexDirection: "column",
          gap: 6,
          minHeight: 0,
        }}
      >
        {shown.length === 0 && (
          <div
            style={{
              fontSize: 9,
              color: "var(--ink3)",
              textAlign: "center",
              padding: "14px 0",
            }}
          >
            no match in the mirrored upstreams
          </div>
        )}
        {shown.map((plugin) => {
          const button = installLabel(plugin);
          return (
            <div
              key={plugin.key}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 12,
                border: "1px solid var(--line2)",
                borderRadius: 6,
                padding: "9px 12px",
              }}
            >
              <span
                aria-hidden
                style={{
                  width: 8,
                  height: 8,
                  background:
                    plugin.category === "trigger"
                      ? "var(--rust)"
                      : "var(--accent)",
                  borderRadius: plugin.category === "trigger" ? 0 : 1,
                  transform:
                    plugin.category === "trigger" ? "rotate(45deg)" : "none",
                  flex: "none",
                }}
              />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div
                  style={{
                    display: "flex",
                    alignItems: "baseline",
                    gap: 8,
                    fontSize: 11,
                  }}
                >
                  <span style={{ fontWeight: 700, color: "var(--ink)" }}>
                    {plugin.name}
                  </span>
                  <span style={{ fontSize: 9, color: "var(--ink3)" }}>
                    {plugin.version}
                  </span>
                  <span
                    style={{
                      fontSize: 9,
                      color: plugin.signed ? "var(--ok)" : "var(--err)",
                    }}
                  >
                    {plugin.signed ? "✓ signed" : "✗ unsigned"}
                  </span>
                </div>
                <div
                  style={{
                    fontSize: 9,
                    color: "var(--ink3)",
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                  }}
                >
                  {plugin.description}
                </div>
                <div
                  style={{ fontSize: 8, color: "var(--ink3)", marginTop: 2 }}
                >
                  {plugin.upstream} · {plugin.installs} installs ·{" "}
                  {plugin.category}
                </div>
              </div>
              <button
                type="button"
                disabled={plugin.installed || plugin.unavailable === true}
                onClick={() => {
                  onInstall(plugin.key);
                }}
                style={{
                  flex: "none",
                  background: button.background,
                  border: `1px solid ${button.border}`,
                  borderRadius: 5,
                  color: button.colour,
                  fontSize: 9,
                  fontWeight: 700,
                  padding: "5px 12px",
                  cursor:
                    plugin.installed || plugin.unavailable === true
                      ? "default"
                      : "pointer",
                }}
              >
                {button.text}
              </button>
            </div>
          );
        })}
      </div>

      <div
        style={{
          padding: "10px 16px",
          borderTop: "1px solid var(--line2)",
          fontSize: 9,
          color: "var(--ink3)",
          lineHeight: 1.6,
          flex: "none",
        }}
      >
        installing resolves the artifact (oci:// or cas://), verifies its
        detached signature against the trust tier&apos;s policy, mirrors it
        locally, and adds it to the catalog. Revisions pin plugins by digest — a
        moved tag never changes what an existing revision runs.
      </div>
    </Modal>
  );
}
