import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { ApiClient } from "./api/client";
import { AppShell } from "./components/AppShell";
import { useAgentState } from "./state/useAgentState";
import "./styles.css";

const api = new ApiClient();

function AgentApplication() {
  const agent = useAgentState({ api });
  return <AppShell agent={agent} />;
}

const root = document.getElementById("root");
if (root === null) {
  throw new Error("The application root element is missing.");
}

createRoot(root).render(
  <StrictMode>
    <AgentApplication />
  </StrictMode>,
);
