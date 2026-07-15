import type { ServiceState } from "../api/types";
import type { AgentConnectionState } from "../state/model";

export interface ConnectionBannerProps {
  connection: AgentConnectionState;
  serviceState: ServiceState | null;
  quitting?: boolean;
  reason?: string | null;
}

interface ConnectionPresentation {
  key: string;
  label: string;
  detail: string;
}

function connectionPresentation({
  connection,
  serviceState,
  quitting,
  reason,
}: ConnectionBannerProps): ConnectionPresentation {
  if (quitting === true || serviceState === "quiescing") {
    return {
      key: "shutting-down",
      label: "Shutting down",
      detail: "The local service is finishing active work and closing safely.",
    };
  }
  if (connection === "session_expired") {
    return {
      key: "session-expired",
      label: "Session expired",
      detail: "Reopen the local application to establish a new protected session.",
    };
  }
  if (connection === "unavailable" || connection === "protocol_error") {
    return {
      key: "server-unavailable",
      label: "Server unavailable",
      detail: reason ?? "The local service cannot be reached right now.",
    };
  }
  if (serviceState === "store_degraded") {
    return {
      key: "store-degraded",
      label: "Store degraded",
      detail: "New work is paused while durable storage recovers.",
    };
  }
  if (
    connection === "bootstrapping" ||
    connection === "reconnecting" ||
    connection === "recovering"
  ) {
    return {
      key: "reconnecting",
      label: "Reconnecting",
      detail: reason ?? "Restoring the live local connection.",
    };
  }
  return {
    key: "connected",
    label: "Connected",
    detail: "Live task updates are connected.",
  };
}

export function ConnectionBanner(props: ConnectionBannerProps) {
  const presentation = connectionPresentation(props);
  return (
    <div
      className={`connection-banner connection-${presentation.key}`}
      role="status"
      aria-live="polite"
      aria-atomic="true"
    >
      <span className="connection-glyph" aria-hidden="true">
        {presentation.key === "connected" ? "●" : "◆"}
      </span>{" "}
      <strong>{presentation.label}</strong>
      <span className="connection-detail">{presentation.detail}</span>
    </div>
  );
}
