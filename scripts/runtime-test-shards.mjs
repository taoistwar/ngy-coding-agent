import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const MANIFEST_PATH = resolve(ROOT, ".github/runtime-test-shards.json");
const ISOLATED_TEST =
  "recovered_branch_refresh_revokes_every_old_delete_capability";

function fail(message) {
  console.error(`Runtime shard validation failed: ${message}`);
  process.exit(1);
}

function parsePositiveInteger(value, option) {
  if (!/^\d+$/.test(value) || Number(value) < 1) {
    fail(`${option} requires a positive integer`);
  }
  return Number(value);
}

function parseArguments(argv) {
  const options = { check: false, run: false, shard: null, expectedShards: null };

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--check") {
      options.check = true;
    } else if (argument === "--run") {
      options.run = true;
    } else if (argument === "--shard") {
      const value = argv[index + 1];
      if (value === undefined) {
        fail("--shard requires a value");
      }
      options.shard = String(parsePositiveInteger(value, "--shard"));
      index += 1;
    } else if (argument === "--expected-shards") {
      const value = argv[index + 1];
      if (value === undefined) {
        fail("--expected-shards requires a value");
      }
      options.expectedShards = parsePositiveInteger(value, "--expected-shards");
      index += 1;
    } else {
      fail(`unknown option ${argument}`);
    }
  }

  if (options.check === options.run) {
    fail("choose exactly one of --check or --run");
  }
  if (options.run && options.shard === null) {
    fail("--run requires --shard");
  }
  if (options.check && options.shard !== null) {
    fail("--shard is only valid with --run");
  }
  if (options.expectedShards === null) {
    fail("--expected-shards is required");
  }

  return options;
}

function readManifest() {
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(MANIFEST_PATH, "utf8"));
  } catch (error) {
    fail(`cannot read ${MANIFEST_PATH}: ${error.message}`);
  }
  return manifest;
}

function cargoMetadata() {
  const result = spawnSync(
    "cargo",
    ["metadata", "--locked", "--offline", "--no-deps", "--format-version", "1"],
    {
      cwd: ROOT,
      encoding: "utf8",
      maxBuffer: 64 * 1024 * 1024,
    },
  );

  if (result.error) {
    fail(`cannot run cargo metadata: ${result.error.message}`);
  }
  if (result.status !== 0) {
    process.stderr.write(result.stderr ?? "");
    fail(`cargo metadata exited with status ${result.status}`);
  }

  try {
    return JSON.parse(result.stdout);
  } catch (error) {
    fail(`cargo metadata returned invalid JSON: ${error.message}`);
  }
}

function validate(manifest, metadata, expectedShards) {
  if (manifest.version !== 1) {
    fail(`unsupported manifest version ${String(manifest.version)}`);
  }
  if (!Array.isArray(manifest.shards)) {
    fail("manifest shards must be an array");
  }
  if (manifest.shards.length !== expectedShards) {
    fail(
      `expected ${expectedShards} shards but manifest defines ${manifest.shards.length}`,
    );
  }

  const runtimePackage = metadata.packages.find(
    (candidate) => candidate.name === "coding-agent-runtime",
  );
  if (!runtimePackage) {
    fail("cargo metadata has no coding-agent-runtime package");
  }

  const libraryTargets = runtimePackage.targets.filter((target) =>
    target.kind.includes("lib"),
  );
  if (libraryTargets.length !== 1) {
    fail(`expected one runtime library target, found ${libraryTargets.length}`);
  }
  const unsupportedTestTargets = runtimePackage.targets.filter(
    (target) =>
      target.test &&
      !target.kind.includes("lib") &&
      !target.kind.includes("test"),
  );
  if (unsupportedTestTargets.length > 0) {
    fail(
      `unsupported runtime test target kinds: ${unsupportedTestTargets
        .map((target) => `${target.name} (${target.kind.join("/")})`)
        .join(", ")}`,
    );
  }

  const discoveredTargets = new Set(
    runtimePackage.targets
      .filter((target) => target.kind.includes("test"))
      .map((target) => target.name),
  );
  const assignedTargets = new Map();
  const shardIds = new Set();
  let libraryAssignments = 0;

  for (const shard of manifest.shards) {
    if (typeof shard.id !== "string" || !/^\d+$/.test(shard.id)) {
      fail("every shard id must be a positive integer string");
    }
    if (shardIds.has(shard.id)) {
      fail(`duplicate shard id ${shard.id}`);
    }
    shardIds.add(shard.id);

    if (typeof shard.includeLib !== "boolean") {
      fail(`shard ${shard.id} includeLib must be a boolean`);
    }
    if (shard.includeLib) {
      libraryAssignments += 1;
    }
    if (!Array.isArray(shard.targets) || shard.targets.length === 0) {
      fail(`shard ${shard.id} must have at least one integration target`);
    }

    for (const target of shard.targets) {
      if (typeof target !== "string" || target.length === 0) {
        fail(`shard ${shard.id} contains an invalid target name`);
      }
      if (assignedTargets.has(target)) {
        fail(
          `target ${target} appears in shards ${assignedTargets.get(target)} and ${shard.id}`,
        );
      }
      assignedTargets.set(target, shard.id);
    }
  }

  if (libraryAssignments !== 1) {
    fail(`runtime library must be assigned once, found ${libraryAssignments}`);
  }

  const missing = [...discoveredTargets].filter(
    (target) => !assignedTargets.has(target),
  );
  const stale = [...assignedTargets.keys()].filter(
    (target) => !discoveredTargets.has(target),
  );
  if (missing.length > 0) {
    fail(`unassigned integration targets: ${missing.sort().join(", ")}`);
  }
  if (stale.length > 0) {
    fail(`unknown integration targets: ${stale.sort().join(", ")}`);
  }

  return { discoveredTargets, shards: manifest.shards };
}

function runCargo(args) {
  console.log(`Running cargo ${args.join(" ")}`);
  const result = spawnSync("cargo", args, { cwd: ROOT, stdio: "inherit" });
  if (result.error) {
    fail(`cannot run cargo: ${result.error.message}`);
  }
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function runShard(shard) {
  if (shard.targets.includes("delivery_cleanup_recovery")) {
    runCargo([
      "test",
      "--locked",
      "--offline",
      "-p",
      "coding-agent-runtime",
      "--test",
      "delivery_cleanup_recovery",
      "--all-features",
      ISOLATED_TEST,
      "--",
      "--exact",
      "--test-threads=1",
      "--nocapture",
    ]);
  }

  const targetArguments = [];
  if (shard.includeLib) {
    targetArguments.push("--lib");
  }
  for (const target of shard.targets) {
    targetArguments.push("--test", target);
  }

  runCargo([
    "test",
    "--locked",
    "--offline",
    "-p",
    "coding-agent-runtime",
    "--all-features",
    ...targetArguments,
    "--",
    "--skip",
    ISOLATED_TEST,
  ]);
}

const options = parseArguments(process.argv.slice(2));
const manifest = readManifest();
const validated = validate(manifest, cargoMetadata(), options.expectedShards);

if (options.check) {
  console.log(
    `Runtime test shards cover ${validated.discoveredTargets.size} integration targets and the library across ${validated.shards.length} shards.`,
  );
} else {
  const shard = validated.shards.find((candidate) => candidate.id === options.shard);
  if (!shard) {
    fail(`manifest has no shard ${options.shard}`);
  }
  runShard(shard);
}
