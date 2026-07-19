# Neon Localhost

Create a temporary Postgres database with [neon.new](https://neon.new) and use it through a familiar local connection on `127.0.0.1:5432`.

Neon Localhost is a native macOS desktop app that:

- provisions an unclaimed, 72-hour Neon database without requiring an account;
- starts a Postgres protocol proxy bound only to the IPv4 and IPv6 localhost interfaces on port `5432`;
- accepts passwordless local connections and handles Neon TLS and authentication inside the proxy;
- provides a claim link if you want to keep the database.

## Run it

```sh
pnpm install
pnpm tauri dev
```

Click **Create local database**, copy the generated URL, and use it anywhere a Postgres connection string is accepted:

```sh
psql 'postgresql://localhost:5432/neondb?sslmode=disable'
```

GUI clients can use `localhost`, port `5432`, database `neondb`, any username, and an empty password. SSL and additional connection options are not required on the local hop.

Only one service can listen on port 5432. Stop any local Postgres or other proxy using that port before creating the database.

## Releases

Create a version commit and annotated tag from a clean `main` branch:

```sh
pnpm release:patch # 0.1.0 -> 0.1.1
pnpm release:minor # 0.1.0 -> 0.2.0
pnpm release:major # 0.1.0 -> 1.0.0
git push origin main --follow-tags
```

The version tag triggers the macOS release workflow. It builds a universal Intel and Apple Silicon DMG, signs it with Developer ID, notarizes the app and DMG, verifies Gatekeeper acceptance, and creates a draft GitHub Release with a SHA-256 checksum. It also signs and publishes the in-app updater package and `latest.json`. Once the draft is published, installed copies check for updates automatically.

The release workflow requires these GitHub Actions secrets:

- `APPLE_CERTIFICATE` and `APPLE_CERTIFICATE_PASSWORD` for a base64-encoded Developer ID Application `.p12`;
- `APPLE_API_KEY_P8`, `APPLE_API_KEY_ID`, and `APPLE_API_ISSUER` for notarization;
- `APPLE_SIGNING_IDENTITY` and a generated `KEYCHAIN_PASSWORD` for the temporary CI keychain.
- `TAURI_SIGNING_PRIVATE_KEY` and `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` for cryptographically verified in-app updates.

The updater public key is embedded in the app. Keep a secure backup of the matching private key: users installed with that public key cannot receive updates signed by a replacement key.

Release jobs create drafts intentionally. Test the downloaded DMG on a second Mac, then publish the draft from GitHub Releases.
