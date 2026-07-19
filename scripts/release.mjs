import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

const root = new URL("../", import.meta.url);
const packageUrl = new URL("package.json", root);
const cargoUrl = new URL("src-tauri/Cargo.toml", root);
const packageJson = JSON.parse(readFileSync(packageUrl, "utf8"));
const current = parseVersion(packageJson.version);
const requested = process.argv[2];

if (!requested) {
  fail("Usage: pnpm release:version -- <patch|minor|major|x.y.z>");
}

const dirty = run("git", ["status", "--porcelain"], { capture: true }).trim();
if (dirty) {
  fail("The working tree must be clean before creating a release.");
}

const next = ["patch", "minor", "major"].includes(requested)
  ? increment(current, requested)
  : parseVersion(requested);

if (compare(next, current) <= 0) {
  fail(`The next version (${format(next)}) must be newer than ${format(current)}.`);
}

const version = format(next);
const tag = `v${version}`;
if (run("git", ["tag", "--list", tag], { capture: true }).trim()) {
  fail(`Tag ${tag} already exists.`);
}

packageJson.version = version;
writeFileSync(packageUrl, `${JSON.stringify(packageJson, null, 2)}\n`);

const cargo = readFileSync(cargoUrl, "utf8");
const updatedCargo = cargo.replace(
  /(^\[package\][\s\S]*?^version\s*=\s*")[^"]+("\s*$)/m,
  `$1${version}$2`,
);
if (updatedCargo === cargo) {
  fail("Could not update the package version in src-tauri/Cargo.toml.");
}
writeFileSync(cargoUrl, updatedCargo);

run("pnpm", ["install", "--lockfile-only"]);
run("cargo", ["check", "--manifest-path", "src-tauri/Cargo.toml"]);
run("pnpm", ["build"]);
run("cargo", ["test", "--manifest-path", "src-tauri/Cargo.toml"]);
run("git", [
  "add",
  "package.json",
  "pnpm-lock.yaml",
  "src-tauri/Cargo.toml",
  "src-tauri/Cargo.lock",
]);
run("git", ["commit", "-m", `chore: release ${tag}`]);
run("git", ["tag", "-a", tag, "-m", `Neon Localhost ${tag}`]);

console.log(`\nCreated ${tag}. Publish it with:\n  git push origin main --follow-tags`);

function parseVersion(value) {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(value ?? "");
  if (!match) fail(`Invalid semantic version: ${value ?? "(missing)"}`);
  return match.slice(1).map(Number);
}

function increment([major, minor, patch], kind) {
  if (kind === "major") return [major + 1, 0, 0];
  if (kind === "minor") return [major, minor + 1, 0];
  return [major, minor, patch + 1];
}

function compare(left, right) {
  for (let index = 0; index < 3; index += 1) {
    if (left[index] !== right[index]) return left[index] - right[index];
  }
  return 0;
}

function format(version) {
  return version.join(".");
}

function run(command, args, { capture = false } = {}) {
  return execFileSync(command, args, {
    cwd: root,
    encoding: "utf8",
    stdio: capture ? ["ignore", "pipe", "inherit"] : "inherit",
  }) ?? "";
}

function fail(message) {
  console.error(`Release aborted: ${message}`);
  process.exit(1);
}
