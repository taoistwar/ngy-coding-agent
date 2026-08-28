import { StrictMode, useEffect } from "react";
import { createRoot } from "react-dom/client";

import { ApiClient } from "./api/client";
import { AuthenticatedTransport } from "./api/authenticatedTransport";
import { DeliveryClient } from "./api/deliveryClient";
import { AppShell } from "./components/AppShell";
import { useAgentState } from "./state/useAgentState";
import { useDeliveryPolling } from "./state/useDeliveryPolling";
import "./styles.css";

const transport = new AuthenticatedTransport();
const api = new ApiClient({ transport });
const deliveryApi = new DeliveryClient({ transport });

function AgentApplication() {
  const agent = useAgentState({ api });
  useEffect(
    () => transport.setSessionExpiredHandler(agent.expireSession),
    [agent.expireSession],
  );
  const delivery = useDeliveryPolling({
    api: deliveryApi,
    taskId: agent.state.selectedTaskId,
  });
  return (
    <AppShell
      agent={agent}
      delivery={{ api: deliveryApi, controller: delivery }}
    />
  );
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
