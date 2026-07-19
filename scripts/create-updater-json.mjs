import { readFileSync, writeFileSync } from "node:fs";
import { basename } from "node:path";

const [version, updaterPath, signaturePath, outputPath] = process.argv.slice(2);

if (!version || !updaterPath || !signaturePath || !outputPath) {
  throw new Error("Usage: node scripts/create-updater-json.mjs <version> <updater> <signature> <output>");
}
if (!/^\d+\.\d+\.\d+$/.test(version)) {
  throw new Error(`Invalid release version: ${version}`);
}

const signature = readFileSync(signaturePath, "utf8").trim();
if (!signature) throw new Error("The updater signature is empty.");

const assetName = basename(updaterPath);
const downloadUrl = `https://github.com/russdias/neon-localhost/releases/download/v${version}/${encodeURIComponent(assetName)}`;
const payload = {
  version,
  notes: `Neon Localhost ${version}`,
  pub_date: new Date().toISOString(),
  platforms: {
    "macos-universal": {
      signature,
      url: downloadUrl,
    },
  },
};

writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`);
