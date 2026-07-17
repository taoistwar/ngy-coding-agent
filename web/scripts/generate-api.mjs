import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const webRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const source = resolve(webRoot, "openapi.json");
const destination = resolve(webRoot, "src/api/generated/schema.d.ts");
const temporary = `${destination}.${process.pid}.tmp`;
const checkOnly = process.argv.slice(2).includes("--check");
const cli = resolve(webRoot, "node_modules/openapi-typescript/bin/cli.js");

mkdirSync(dirname(destination), { recursive: true });
rmSync(temporary, { force: true });

try {
  const result = spawnSync(process.execPath, [cli, source, "--output", temporary], {
    cwd: webRoot,
    stdio: "inherit",
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    const outcome = result.signal ?? `exit status ${result.status ?? "unknown"}`;
    throw new Error(`openapi-typescript failed with ${outcome}`);
  }

  const generated = Buffer.from(
    readFileSync(temporary, "utf8").replace(/\r\n?|\n/g, "\n"),
    "utf8",
  );
  writeFileSync(temporary, generated);
  const current = existsSync(destination)
    ? Buffer.from(
        readFileSync(destination, "utf8").replace(/\r\n?|\n/g, "\n"),
        "utf8",
      )
    : null;

  if (current === null || !current.equals(generated)) {
    if (checkOnly) {
      console.error("Generated OpenAPI types are out of date. Run npm run api:generate.");
      process.exitCode = 1;
    } else {
      // The temporary file is a sibling, so rename is an atomic same-volume
      // replacement. If it fails, the committed destination remains intact.
      renameSync(temporary, destination);
    }
  }
} finally {
  rmSync(temporary, { force: true });
}
