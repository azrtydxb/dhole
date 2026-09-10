/**
 * The shell the canvas hangs off.
 *
 * Which pipeline and which revision are read from the query string, because
 * there is no pipeline list yet and inventing one here would be a screen the
 * plan has not reached. A revision is REQUIRED rather than defaulted: every
 * edit is applied against a base revision, and a canvas that guessed one would
 * be guessing whose work it is about to overwrite.
 */
import { Canvas } from "./canvas/Canvas.js";
import { RunView } from "./run/RunView.js";
import { getToken } from "./api/client.js";
import { useLocationKey } from "./useLocation.js";

/** App picks the pipeline to edit and hands it to the canvas. */
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
      <main>
        <h1>Dhole</h1>
        <p>No API token stored - sign in to load pipelines.</p>
      </main>
    );
  }

  // A run is a different thing to look at, not a different app: the run view
  // reads the realised graph and the logs, and the canvas edits a definition.
  if (runId !== "") {
    return (
      <main>
        <h1>Dhole</h1>
        <RunView runId={runId} />
      </main>
    );
  }

  if (pipelineId === "" || revisionId === "") {
    return (
      <main>
        <h1>Dhole</h1>
        <p>
          Signed in. Open a pipeline with{" "}
          <code>?pipeline=ID&amp;revision=REV</code>, or a run with{" "}
          <code>?run=ID</code>.
        </p>
      </main>
    );
  }

  return (
    <main>
      <h1>Dhole</h1>
      <Canvas pipelineId={pipelineId} revisionId={revisionId} />
    </main>
  );
}
