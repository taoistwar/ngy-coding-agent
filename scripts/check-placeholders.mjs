import { execFileSync } from "node:child_process";
import { lstatSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { TextDecoder } from "node:util";

const SELF = "scripts/check-placeholders.mjs";
const IMPLEMENTATION_PLAN = /^docs\/superpowers\/plans\/.*\.md$/;
const FORBIDDEN_MARKERS = [
  { label: "TODO", expression: /\bTODO\b/g },
  { label: "FIXME", expression: /\bFIXME\b/g },
  { label: "XXX", expression: /\bXXX\b/g },
  { label: "TBD", expression: /\bTBD\b/g },
  { label: "HACK", expression: /\bHACK\b/g },
  { label: "STUB", expression: /\bSTUB\b/g },
  { label: "PLACEHOLDER", expression: /\bPLACEHOLDER\b/g },
  { label: "todo!", expression: /\btodo\s*!\s*\(/g },
  { label: "unimplemented!", expression: /\bunimplemented\s*!\s*\(/g },
  { label: "NotImplementedError", expression: /\bNotImplementedError\b/g },
  { label: "IMPLEMENT_ME", expression: /\bIMPLEMENT(?:[ _-]?ME)\b/gi },
  { label: "COMING_SOON", expression: /\bCOMING(?:[ _-]?SOON)\b/gi },
  { label: "NOT_IMPLEMENTED", expression: /\bNOT(?:[ _-]?IMPLEMENTED)\b/gi },
];

const decoder = new TextDecoder("utf-8", { fatal: true });

function trackedAndUntrackedFiles() {
  const output = execFileSync(
    "git",
    ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
    { encoding: "buffer", maxBuffer: 64 * 1024 * 1024 },
  );

  return output
    .toString("utf8")
    .split("\0")
    .filter(Boolean)
    .map((path) => path.replaceAll("\\", "/"));
}

function shouldExclude(path) {
  return path === SELF || IMPLEMENTATION_PLAN.test(path);
}

function readText(path) {
  let stats;
  try {
    stats = lstatSync(resolve(path));
  } catch (error) {
    if (error.code === "ENOENT") {
      return { kind: "missing" };
    }
    throw error;
  }

  if (!stats.isFile()) {
    return { kind: "non-file" };
  }

  const buffer = readFileSync(resolve(path));
  if (looksBinary(buffer)) {
    return { kind: "binary" };
  }

  try {
    return { kind: "text", text: decoder.decode(buffer) };
  } catch {
    return { kind: "binary" };
  }
}

function looksBinary(buffer) {
  for (const byte of buffer.subarray(0, 8 * 1024)) {
    const isTextWhitespace = byte === 0x09 || byte === 0x0a || byte === 0x0d;
    if (!isTextWhitespace && (byte < 0x20 || byte === 0x7f)) {
      return true;
    }
  }
  return false;
}

function locate(text, index) {
  const prefix = text.slice(0, index);
  const line = prefix.split("\n").length;
  const previousNewline = prefix.lastIndexOf("\n");
  return { line, column: index - previousNewline };
}

const findings = [];
let scannedTextFiles = 0;
let skippedBinaryFiles = 0;

for (const path of trackedAndUntrackedFiles()) {
  if (shouldExclude(path)) {
    continue;
  }

  const file = readText(path);
  if (file.kind === "binary") {
    skippedBinaryFiles += 1;
    continue;
  }
  if (file.kind !== "text") {
    continue;
  }

  scannedTextFiles += 1;
  for (const marker of FORBIDDEN_MARKERS) {
    marker.expression.lastIndex = 0;
    for (const match of file.text.matchAll(marker.expression)) {
      const { line, column } = locate(file.text, match.index);
      findings.push({ path, line, column, marker: marker.label });
    }
  }
}

if (findings.length > 0) {
  console.error("Forbidden placeholder markers found:");
  for (const finding of findings) {
    console.error(
      `${finding.path}:${finding.line}:${finding.column}: ${finding.marker}`,
    );
  }
  process.exitCode = 1;
} else {
  console.log(
    `Placeholder check passed (${scannedTextFiles} text files scanned, ${skippedBinaryFiles} binary files skipped).`,
  );
}
