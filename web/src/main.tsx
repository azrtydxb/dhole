import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App.js";
import "./design/theme.css";
import "@xyflow/react/dist/style.css";

const queryClient = new QueryClient();

const container = document.getElementById("root");
if (container === null) {
  throw new Error("index.html is missing its #root element");
}

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
