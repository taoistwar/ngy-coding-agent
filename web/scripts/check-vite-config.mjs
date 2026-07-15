import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";

import { loadConfigFromFile } from "vite";

const configFile = fileURLToPath(new URL("../vite.config.ts", import.meta.url));
const targetVariable = "CODING_AGENT_AXUM_TARGET";

async function loadConfig(command, target) {
  const previous = process.env[targetVariable];

  if (target === undefined) {
    delete process.env[targetVariable];
  } else {
    process.env[targetVariable] = target;
  }

  try {
    const loaded = await loadConfigFromFile(
      { command, mode: command === "build" ? "production" : "development" },
      configFile,
      undefined,
      "silent",
    );
    assert.ok(loaded, "Vite config must load");
    return loaded.config;
  } finally {
    if (previous === undefined) {
      delete process.env[targetVariable];
    } else {
      process.env[targetVariable] = previous;
    }
  }
}

const buildConfig = await loadConfig("build");
assert.deepEqual(buildConfig.build, {
  outDir: "dist",
  emptyOutDir: true,
  manifest: true,
});
assert.equal(buildConfig.server, undefined, "production must not infer a proxy target");

await assert.rejects(
  () => loadConfig("serve"),
  new RegExp(targetVariable),
  "development must require an explicit Axum target",
);

for (const invalidTarget of [
  "http://localhost:43121",
  "http://0.0.0.0:43121",
  "http://127.0.0.2:43121",
  "https://127.0.0.1:43121",
  "http://127.0.0.1:0",
  "http://127.0.0.1:65536",
  "http://127.0.0.1:43121/",
  "http://127.0.0.1:43121/api",
  "http://127.0.0.1:43121?query=yes",
  "http://127.0.0.1:43121#fragment",
  "http://user:pass@127.0.0.1:43121",
]) {
  await assert.rejects(
    () => loadConfig("serve", invalidTarget),
    new RegExp(targetVariable),
    `development must reject ${invalidTarget}`,
  );
}

const target = "http://127.0.0.1:43121";
const developmentConfig = await loadConfig("serve", target);
assert.equal(developmentConfig.server?.host, "127.0.0.1");
assert.equal(developmentConfig.server?.port, 5173);
assert.equal(developmentConfig.server?.strictPort, true);

const proxy = developmentConfig.server?.proxy;
assert.ok(proxy && typeof proxy === "object", "development proxy must exist");
assert.deepEqual(Object.keys(proxy).sort(), ["^/_local(?:/|\\?|$)", "^/api(?:/|\\?|$)"]);

for (const [namespace, queryUrl] of [
  ["api", "/api?probe=ready"],
  ["_local", "/_local?probe=ready"],
]) {
  const pattern = Object.keys(proxy).find((candidate) => candidate.includes(namespace));
  assert.ok(pattern, `${namespace} proxy pattern must exist`);
  assert.match(queryUrl, new RegExp(pattern), `${queryUrl} must stay in the backend namespace`);
}

for (const options of Object.values(proxy)) {
  assert.equal(typeof options, "object");
  assert.equal(options.target, target);
  assert.equal(options.changeOrigin, true, "Axum must receive its exact Host authority");
  assert.equal(options.timeout, 0, "SSE must not be cut off by a request timeout");
  assert.equal(options.proxyTimeout, 0, "SSE must not be cut off by a proxy timeout");
  assert.equal(options.rewrite, undefined, "the protected route path must be preserved");
  assert.equal(options.configure, undefined, "the proxy must not buffer streamed responses");
}

console.log("Vite build and explicit development proxy contract is valid.");
