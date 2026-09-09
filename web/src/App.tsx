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
import { getToken } from "./api/client.js";

/** App picks the pipeline to edit and hands it to the canvas. */
export function App() {
  const signedIn = getToken() !== null;
  const parameters = new URLSearchParams(globalThis.location?.search ?? "");
  const pipelineId = parameters.get("pipeline") ?? "";
  const revisionId = parameters.get("revision") ?? "";

  if (!signedIn) {
    return (
      <main>
        <h1>Dhole</h1>
        <p>No API token stored - sign in to load pipelines.</p>
      </main>
    );
  }

  if (pipelineId === "" || revisionId === "") {
    return (
      <main>
        <h1>Dhole</h1>
        <p>
          Signed in. Open a pipeline with{" "}
          <code>?pipeline=ID&amp;revision=REV</code>.
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
