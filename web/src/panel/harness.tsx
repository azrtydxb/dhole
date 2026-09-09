/**
 * The module that mounts the property panel on the running application page.
 *
 * The application shell (src/App.tsx) has no panel route yet: it opens the
 * canvas, and the screen that puts the two together belongs to a later task.
 * This module is how the panel is opened against a real control plane in the
 * meantime — the same providers main.tsx installs, the same API client, and no
 * behaviour of its own. It goes away the day App routes to the panel, and
 * nothing but the e2e suite loads it.
 *
 * It mounts into its own container rather than #root so it can be added to the
 * page the app already serves, and reads query keys of its own ("panel.step"
 * and friends) so that opening it does not also open a canvas on the same
 * parameters.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { PropertyPanel } from "./PropertyPanel.js";

const queryClient = new QueryClient();
const parameters = new URLSearchParams(globalThis.location?.search ?? "");

const existing = document.getElementById("panel-root");
const container = existing ?? document.createElement("div");
container.id = "panel-root";
if (existing === null) {
  document.body.append(container);
}

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <PropertyPanel
        pipelineId={parameters.get("panel.pipeline") ?? ""}
        revisionId={parameters.get("panel.revision") ?? ""}
        stepId={parameters.get("panel.step") ?? ""}
      />
    </QueryClientProvider>
  </StrictMode>,
);
