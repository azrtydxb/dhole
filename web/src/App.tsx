/**
 * The shell the canvas (Task 46) will hang off. It shows only what can be
 * proven today: whether this browser holds a token for the control plane.
 */
import { getToken } from "./api/client.js";

export function App() {
  const signedIn = getToken() !== null;
  return (
    <main>
      <h1>Dhole</h1>
      <p>
        {signedIn
          ? "Signed in."
          : "No API token stored — sign in to load pipelines."}
      </p>
    </main>
  );
}
