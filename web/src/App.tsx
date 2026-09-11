/**
 * The shell the canvas hangs off.
 *
 * Which pipeline and which revision are read from the query string, because
 * there is no pipeline list yet and inventing one here would be a screen the
 * plan has not reached. A revision is REQUIRED rather than defaulted: every
 * edit is applied against a base revision, and a canvas that guessed one would
 * be guessing whose work it is about to overwrite.
 *
 * The three screens below are the three states this app has: signed out, a run
 * to read, and a revision to edit. Each renders the editor's typography and
 * tokens rather than the browser's defaults, so the signed-out screen looks
 * like the same product as the editor behind it.
 */
import { Editor } from "./shell/Editor.js";
import { RunView } from "./run/RunView.js";
import { getToken } from "./api/client.js";
import { useLocationKey } from "./useLocation.js";

/** Frame is the plain, centred page the two non-editor states use. */
function Frame({ children }: { readonly children: React.ReactNode }) {
  return (
    <main
      style={{
        height: "100vh",
        background: "var(--bg)",
        color: "var(--ink)",
        fontFamily: "var(--font)",
        padding: "48px 32px",
        boxSizing: "border-box",
        overflowY: "auto",
      }}
    >
      <div style={{ maxWidth: 720, margin: "0 auto" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 9,
            marginBottom: 24,
          }}
        >
          <span
            aria-hidden
            style={{
              width: 11,
              height: 11,
              background: "var(--rust)",
              transform: "rotate(45deg)",
              borderRadius: 2,
            }}
          />
          <h1 style={{ fontSize: 16, margin: 0 }}>Dhole</h1>
        </div>
        {children}
      </div>
    </main>
  );
}

/** App picks the pipeline to edit and hands it to the editor. */
export function App() {
  // Subscribed to rather than read: see useLocationKey. Its value is not used
  // directly — reading it is what makes this component re-render when the URL
  // moves, so the reads below see the new one.
  useLocationKey();

  const signedIn = getToken() !== null;
  const parameters = new URLSearchParams(globalThis.location?.search ?? "");
  const pipelineId = parameters.get("pipeline") ?? "";
  const revisionId = parameters.get("revision") ?? "";
  // A run is addressed by hash route, `#/runs/<id>`, so a link to a run
  // survives being pasted somewhere that strips a query string.
  const hash = globalThis.location?.hash ?? "";
  const runId =
    /^#\/runs\/([^/?#]+)/.exec(hash)?.[1] ?? parameters.get("run") ?? "";

  if (!signedIn) {
    return (
      <Frame>
        <p style={{ fontSize: 12, color: "var(--ink2)", lineHeight: 1.7 }}>
          No API token stored — sign in to load pipelines.
        </p>
      </Frame>
    );
  }

  // A run WITH a pipeline is followed on the editor's own canvas: the graph is
  // where "which step is this stuck on" is legible, and sending the user to a
  // separate screen takes away the thing they were reading. A run on its own —
  // a pasted link, no pipeline in the URL — still gets the run view, because
  // there is no definition to draw it over.
  if (runId !== "" && pipelineId !== "" && revisionId !== "") {
    return (
      <Editor
        pipelineId={pipelineId}
        revisionId={revisionId}
        tenant={parameters.get("tenant") ?? "default"}
        user={parameters.get("as") ?? "me"}
        runId={runId}
      />
    );
  }

  if (runId !== "") {
    return (
      <Frame>
        <RunView runId={runId} />
      </Frame>
    );
  }

  if (pipelineId === "" || revisionId === "") {
    return (
      <Frame>
        <p style={{ fontSize: 12, color: "var(--ink2)", lineHeight: 1.7 }}>
          Signed in. Open a pipeline with{" "}
          <code>?pipeline=ID&amp;revision=REV</code>, or a run with{" "}
          <code>?run=ID</code>.
        </p>
      </Frame>
    );
  }

  return (
    <Editor
      pipelineId={pipelineId}
      revisionId={revisionId}
      // The tenant and the person are the credential's, not the URL's. Until
      // the token carries them in a form this app can read, they are shown as
      // unknown rather than guessed: a breadcrumb naming the wrong tenant is
      // worse than one naming none.
      tenant={parameters.get("tenant") ?? "default"}
      user={parameters.get("as") ?? "me"}
    />
  );
}
