import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const AXUM_TARGET_ENV = "CODING_AGENT_AXUM_TARGET";
const DEVELOPMENT_HOST = "127.0.0.1";
const DEVELOPMENT_PORT = 5173;

function explicitAxumTarget(): string {
  const configured = process.env[AXUM_TARGET_ENV];
  if (!configured) {
    throw new Error(
      `${AXUM_TARGET_ENV} must be an explicit Axum origin such as http://127.0.0.1:43121`,
    );
  }

  let target: URL;
  try {
    target = new URL(configured);
  } catch {
    throw new Error(
      `${AXUM_TARGET_ENV} must be an explicit Axum origin such as http://127.0.0.1:43121`,
    );
  }

  const port = Number(target.port);
  const isBareLoopbackOrigin =
    target.protocol === "http:" &&
    target.hostname === DEVELOPMENT_HOST &&
    target.port !== "" &&
    Number.isInteger(port) &&
    port >= 1 &&
    port <= 65_535 &&
    target.origin === configured;

  if (!isBareLoopbackOrigin) {
    throw new Error(
      `${AXUM_TARGET_ENV} must be a bare http://127.0.0.1:<nonzero-port> origin`,
    );
  }

  return target.origin;
}

function streamingProxy(target: string) {
  return {
    target,
    changeOrigin: true,
    timeout: 0,
    proxyTimeout: 0,
  };
}

export default defineConfig(({ command }) => {
  const build = {
    outDir: "dist",
    emptyOutDir: true,
    manifest: true,
  };

  if (command === "build") {
    return {
      plugins: [react()],
      build,
    };
  }

  const target = explicitAxumTarget();
  return {
    plugins: [react()],
    build,
    server: {
      host: DEVELOPMENT_HOST,
      port: DEVELOPMENT_PORT,
      strictPort: true,
      proxy: {
        "^/api(?:/|\\?|$)": streamingProxy(target),
        "^/_local(?:/|\\?|$)": streamingProxy(target),
      },
    },
  };
});
