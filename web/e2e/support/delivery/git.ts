import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { lstat, readFile, realpath, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const OID = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/u;

export interface DeliveryTargetSnapshot {
  readonly branch: string;
  readonly head: string;
  readonly tree: string;
  readonly status: string;
  readonly files: Readonly<Record<string, string>>;
}

export interface ExactNoFfMerge {
  readonly commit: string;
  readonly firstParent: string;
  readonly secondParent: string;
  readonly tree: string;
}

/** Git-only assertions for one isolated delivery fixture repository. */
export class DeliveryGit {
  readonly repositoryDir: string;

  private constructor(
    repositoryDir: string,
    private readonly fixtureRoot: string,
  ) {
    this.repositoryDir = repositoryDir;
  }

  static async open(repositoryDir: string, fixtureRoot: string): Promise<DeliveryGit> {
    const [canonical, canonicalRoot] = await Promise.all([
      realpath(repositoryDir),
      realpath(fixtureRoot),
    ]);
    requireDescendant(canonicalRoot, canonical, "delivery fixture repository");
    const git = new DeliveryGit(canonical, canonicalRoot);
    const inside = await git.line(["rev-parse", "--is-inside-work-tree"]);
    if (inside !== "true") throw new Error("delivery fixture is not a Git worktree");
    return git;
  }

  async snapshotTarget(): Promise<DeliveryTargetSnapshot> {
    const [branch, head, tree, status, files] = await Promise.all([
      this.line(["symbolic-ref", "--quiet", "--short", "HEAD"]),
      this.line(["rev-parse", "HEAD"]),
      this.line(["rev-parse", "HEAD^{tree}"]),
      this.output(["status", "--porcelain=v1", "--untracked-files=all"]),
      this.fileDigests(),
    ]);
    requireOid(head, "target HEAD");
    requireOid(tree, "target tree");
    return Object.freeze({ branch, head, tree, status, files: Object.freeze(files) });
  }

  async assertTargetUnchanged(expected: DeliveryTargetSnapshot): Promise<void> {
    const actual = await this.snapshotTarget();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw new Error(
        `delivery preflight changed the target checkout\nexpected=${JSON.stringify(expected)}\nactual=${JSON.stringify(actual)}`,
      );
    }
  }

  async assertExactNoFfMerge(
    before: DeliveryTargetSnapshot,
    expectedDurableSourceCommit: string,
  ): Promise<ExactNoFfMerge> {
    requireOid(expectedDurableSourceCommit, "expected durable source commit");
    const [commit, parentLine, tree, status, branch] = await Promise.all([
      this.line(["rev-parse", "HEAD"]),
      this.line(["show", "-s", "--format=%P", "HEAD"]),
      this.line(["rev-parse", "HEAD^{tree}"]),
      this.output(["status", "--porcelain=v1", "--untracked-files=all"]),
      this.line(["symbolic-ref", "--quiet", "--short", "HEAD"]),
    ]);
    const parents = parentLine.split(/\s+/u).filter(Boolean);
    if (parents.length !== 2) {
      throw new Error(`delivery result is not an exact two-parent no-ff merge: ${parentLine}`);
    }
    const [firstParent, secondParent] = parents;
    if (firstParent !== before.head || secondParent !== expectedDurableSourceCommit) {
      throw new Error(
        `delivery merge parents changed (expected ${before.head} ${expectedDurableSourceCommit}, got ${parentLine})`,
      );
    }
    if (branch !== before.branch) {
      throw new Error(`delivery merge changed checkout branch from ${before.branch} to ${branch}`);
    }
    if (status !== "") throw new Error(`delivery merge left a dirty target checkout: ${status}`);
    requireOid(commit, "merge commit");
    requireOid(tree, "merge tree");
    return Object.freeze({ commit, firstParent, secondParent, tree });
  }

  async sourceRefs(): Promise<Readonly<Record<string, string>>> {
    const output = await this.output([
      "for-each-ref",
      "--format=%(refname)%00%(objectname)",
      "refs/heads/codex/",
    ]);
    const refs: Record<string, string> = {};
    for (const line of output.split(/\r?\n/u).filter(Boolean)) {
      const [name, oid, extra] = line.split("\u0000");
      if (name === undefined || oid === undefined || extra !== undefined) {
        throw new Error("Git returned a malformed source-ref record");
      }
      requireOid(oid, `source ref ${name}`);
      refs[name] = oid;
    }
    return Object.freeze(refs);
  }

  async refOid(reference: string): Promise<string | null> {
    validateRef(reference);
    const result = await this.run(
      ["rev-parse", "--verify", "--quiet", "--end-of-options", reference],
      true,
    );
    if (result.exitCode === 1 && result.stdout === "") return null;
    if (result.exitCode !== 0) {
      throw new Error(`git rev-parse failed (${String(result.exitCode)}): ${result.stderr}`);
    }
    const oid = result.stdout.trim();
    requireOid(oid, reference);
    return oid;
  }

  /** Removes only the Cargo output created by the production runner in its exact source worktree. */
  async removeFixtureCargoTarget(sourceRef: string, expectedSourceOid: string): Promise<void> {
    validateRef(sourceRef);
    requireOid(expectedSourceOid, "expected source worktree HEAD");
    const sourceWorktree = await this.sourceWorktree(sourceRef, expectedSourceOid);
    const metadata = await cargoMetadata(sourceWorktree);
    const workspaceRoot = await realpath(metadata.workspace_root);
    if (!samePath(workspaceRoot, sourceWorktree)) {
      throw new Error("fixture Cargo workspace root did not equal the exact source worktree");
    }

    const targetPath = path.resolve(metadata.target_directory);
    if (!samePath(targetPath, path.join(sourceWorktree, "target"))) {
      throw new Error("fixture Cargo target directory was not the workspace-local target");
    }
    const targetLinkStat = await lstat(targetPath);
    if (!targetLinkStat.isDirectory() || targetLinkStat.isSymbolicLink()) {
      throw new Error("fixture Cargo target was not a real directory");
    }
    const target = await realpath(targetPath);
    requireDescendant(sourceWorktree, target, "fixture Cargo target directory");
    requireDescendant(this.fixtureRoot, target, "fixture Cargo target directory");
    if (!samePath(target, targetPath)) {
      throw new Error("fixture Cargo target resolved through an unexpected filesystem alias");
    }

    await rm(target, { recursive: true });
    await assertPathAbsent(target, "fixture Cargo target");
    const status = await this.outputAt(sourceWorktree, [
      "status",
      "--porcelain=v2",
      "--ignored=matching",
      "--untracked-files=all",
    ]);
    if (status !== "") {
      throw new Error(`source worktree remained dirty after Cargo target cleanup: ${status}`);
    }
  }

  async writeFixtureFile(relativePath: string, contents: string): Promise<void> {
    const target = this.fixturePath(relativePath);
    await writeFile(target, contents, "utf8");
  }

  async readFixtureFile(relativePath: string): Promise<Buffer> {
    return readFile(this.fixturePath(relativePath));
  }

  async commitAll(message: string): Promise<string> {
    if (message.trim() === "" || message.includes("\u0000")) {
      throw new Error("fixture commit message must be a non-empty text value");
    }
    await this.git(["add", "--all", "--", "."]);
    await this.git([
      "-c",
      "user.name=NGY Delivery E2E",
      "-c",
      "user.email=delivery-e2e@example.invalid",
      "commit",
      "-m",
      message,
    ]);
    return this.line(["rev-parse", "HEAD"]);
  }

  async setLocalConfig(key: string, value: string): Promise<void> {
    validateConfigKey(key);
    if (value === "" || value.includes("\u0000")) {
      throw new Error("fixture Git config value must be non-empty text");
    }
    await this.git(["config", "--local", "--no-includes", key, value]);
  }

  async unsetLocalConfig(key: string): Promise<void> {
    validateConfigKey(key);
    await this.git(["config", "--local", "--no-includes", "--unset-all", key]);
  }

  async line(arguments_: readonly string[]): Promise<string> {
    const output = await this.output(arguments_);
    const lines = output.trim().split(/\r?\n/u);
    if (lines.length !== 1 || lines[0] === "") {
      throw new Error(`git ${arguments_.join(" ")} did not return exactly one line`);
    }
    return lines[0] as string;
  }

  async output(arguments_: readonly string[]): Promise<string> {
    return this.outputAt(this.repositoryDir, arguments_);
  }

  private async outputAt(cwd: string, arguments_: readonly string[]): Promise<string> {
    const result = await this.run(arguments_, false, cwd);
    if (result.exitCode !== 0) {
      throw new Error(
        `git ${arguments_.join(" ")} failed (${String(result.exitCode)}): ${result.stderr}`,
      );
    }
    return result.stdout;
  }

  private async git(arguments_: readonly string[]): Promise<void> {
    await this.output(arguments_);
  }

  private async run(
    arguments_: readonly string[],
    allowFailure: boolean,
    cwd = this.repositoryDir,
  ): Promise<{ exitCode: number; stdout: string; stderr: string }> {
    if (arguments_.length === 0 || arguments_.some((value) => value.includes("\u0000"))) {
      throw new Error("invalid Git argument list");
    }
    try {
      const result = await execFileAsync("git", [...arguments_], {
        cwd,
        encoding: "utf8",
        env: {
          ...process.env,
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_TERMINAL_PROMPT: "0",
        },
        maxBuffer: 4 * 1024 * 1024,
        windowsHide: true,
      });
      return { exitCode: 0, stdout: result.stdout, stderr: result.stderr };
    } catch (error) {
      const failure = error as NodeJS.ErrnoException & {
        code?: number | string;
        stdout?: string;
        stderr?: string;
      };
      const exitCode = typeof failure.code === "number" ? failure.code : -1;
      if (!allowFailure || exitCode < 0) throw error;
      return {
        exitCode,
        stdout: failure.stdout ?? "",
        stderr: failure.stderr ?? failure.message,
      };
    }
  }

  private async sourceWorktree(sourceRef: string, expectedSourceOid: string): Promise<string> {
    const records = parseWorktreeList(await this.output(["worktree", "list", "--porcelain", "-z"]));
    const matching = records.filter((record) => record.branch === sourceRef);
    if (matching.length !== 1) {
      throw new Error("delivery source ref did not identify one exact Git worktree");
    }
    const [record] = matching;
    if (record === undefined || record.head !== expectedSourceOid) {
      throw new Error("delivery source worktree HEAD did not match the retained source OID");
    }
    const canonical = await realpath(record.worktree);
    requireDescendant(this.fixtureRoot, canonical, "delivery source worktree");
    if (samePath(canonical, this.repositoryDir)) {
      throw new Error("delivery source worktree unexpectedly resolved to the target checkout");
    }
    return canonical;
  }

  private fixturePath(relativePath: string): string {
    if (
      relativePath === "" ||
      path.isAbsolute(relativePath) ||
      relativePath.includes("\u0000")
    ) {
      throw new Error("fixture path must be a non-empty relative path");
    }
    const target = path.resolve(this.repositoryDir, relativePath);
    const relative = path.relative(this.repositoryDir, target);
    if (relative.startsWith("..") || path.isAbsolute(relative) || relative === ".git") {
      throw new Error("fixture path escaped the repository root");
    }
    return target;
  }

  private async fileDigests(): Promise<Record<string, string>> {
    const encoded = await this.output([
      "ls-files",
      "--cached",
      "--others",
      "--exclude-standard",
      "-z",
    ]);
    const files = encoded.split("\u0000").filter(Boolean).sort();
    const digests: Record<string, string> = {};
    for (const name of files) {
      const bytes = await readFile(this.fixturePath(name));
      digests[name] = createHash("sha256").update(bytes).digest("hex");
    }
    return digests;
  }
}

interface CargoMetadata {
  readonly workspace_root: string;
  readonly target_directory: string;
}

interface WorktreeRecord {
  readonly worktree: string;
  readonly head: string;
  readonly branch: string | null;
}

async function cargoMetadata(workspace: string): Promise<CargoMetadata> {
  const result = await execFileAsync(
    "cargo",
    ["metadata", "--locked", "--offline", "--no-deps", "--format-version", "1"],
    {
      cwd: workspace,
      encoding: "utf8",
      env: { ...process.env, CARGO_NET_OFFLINE: "true" },
      maxBuffer: 4 * 1024 * 1024,
      windowsHide: true,
    },
  );
  const value = JSON.parse(result.stdout) as unknown;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("fixture Cargo metadata was not an object");
  }
  const metadata = value as Record<string, unknown>;
  if (typeof metadata.workspace_root !== "string" || typeof metadata.target_directory !== "string") {
    throw new Error("fixture Cargo metadata omitted workspace paths");
  }
  return Object.freeze({
    workspace_root: metadata.workspace_root,
    target_directory: metadata.target_directory,
  });
}

function parseWorktreeList(output: string): readonly WorktreeRecord[] {
  const encodedRecords = output.split("\u0000\u0000");
  if (encodedRecords.at(-1) !== "") {
    throw new Error("Git worktree list omitted its terminating record separator");
  }
  const records = encodedRecords.slice(0, -1).map((encoded) => {
    const fields = new Map<string, string>();
    for (const field of encoded.split("\u0000")) {
      const separator = field.indexOf(" ");
      const key = separator === -1 ? field : field.slice(0, separator);
      const value = separator === -1 ? "" : field.slice(separator + 1);
      if (key === "" || fields.has(key)) {
        throw new Error("Git worktree list contained an invalid or duplicate field");
      }
      fields.set(key, value);
    }
    const worktree = fields.get("worktree");
    const head = fields.get("HEAD");
    const branch = fields.get("branch") ?? null;
    if (worktree === undefined || worktree === "" || head === undefined) {
      throw new Error("Git worktree list omitted worktree identity");
    }
    requireOid(head, `worktree HEAD for ${worktree}`);
    if (branch !== null) validateRef(branch);
    return Object.freeze({ worktree, head, branch });
  });
  if (records.length === 0) throw new Error("Git worktree list was empty");
  return Object.freeze(records);
}

function requireDescendant(root: string, candidate: string, label: string): void {
  const relative = path.relative(root, candidate);
  if (
    relative === "" ||
    relative.startsWith(`..${path.sep}`) ||
    relative === ".." ||
    path.isAbsolute(relative)
  ) {
    throw new Error(`${label} escaped its fixture root`);
  }
}

function samePath(left: string, right: string): boolean {
  return process.platform === "win32"
    ? path.resolve(left).toLowerCase() === path.resolve(right).toLowerCase()
    : path.resolve(left) === path.resolve(right);
}

async function assertPathAbsent(candidate: string, label: string): Promise<void> {
  try {
    await lstat(candidate);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  throw new Error(`${label} still existed after cleanup`);
}

function requireOid(candidate: string, label: string): void {
  if (!OID.test(candidate) || /^0+$/u.test(candidate)) {
    throw new Error(`${label} is not a canonical non-zero Git object ID`);
  }
}

function validateRef(reference: string): void {
  if (!reference.startsWith("refs/heads/") || reference.includes("\u0000")) {
    throw new Error("delivery source reference is not a local branch ref");
  }
}

function validateConfigKey(key: string): void {
  if (!/^[A-Za-z][A-Za-z0-9.-]*$/u.test(key)) {
    throw new Error("fixture Git config key is not canonical");
  }
}
