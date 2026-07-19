# Neon Localhost

### Cloud Postgres. Localhost simple.

Neon Localhost turns a temporary [neon.new](https://neon.new) database into a familiar, passwordless Postgres connection on your Mac:

```text
postgresql://localhost:5432/neondb?sslmode=disable
```

No account. No Docker. No local Postgres installation. Create a database, point your app at `localhost:5432`, and start building.

**[Download the latest version for macOS →](https://github.com/russdias/neon-localhost/releases/latest)**

![Neon Localhost connected to a temporary Neon database on macOS](https://github.com/user-attachments/assets/5c025ee8-e8e6-45c3-9e25-aebd7a207527)

## Why Neon Localhost?

Cloud databases are convenient, but local development tools expect local database ergonomics. Neon Localhost bridges the two:

- **Starts in seconds** — provisions a temporary Neon Postgres database without an account.
- **Works with existing tools** — use the same `localhost:5432` connection in your app, ORM, migrations, `psql`, TablePlus, Postico, or any other Postgres client.
- **No local credentials** — connect without a password; the app manages the remote Neon credentials for you.
- **Secure beyond your Mac** — the proxy listens only on localhost and encrypts the connection from your Mac to Neon with TLS.
- **Disposable by default** — experiment freely for 72 hours, then claim the database if you want to keep it.
- **Feels at home on macOS** — native desktop experience, Light and Dark appearances, storage visibility, connection status, and automatic updates.

## Get started

1. [Download the latest DMG](https://github.com/russdias/neon-localhost/releases/latest), open it, and drag Neon Localhost to Applications.
2. Open the app and choose **Create Database**.
3. Copy the local URL into your project or connect directly:

```sh
psql 'postgresql://localhost:5432/neondb?sslmode=disable'
```

For GUI clients, use:

| Setting | Value |
| --- | --- |
| Host | `localhost` |
| Port | `5432` |
| Database | `neondb` |
| Username | Any value |
| Password | Leave empty |
| SSL | Disable for the local connection |

Your client talks to Neon Localhost without credentials or TLS. The app authenticates upstream and establishes the encrypted Neon connection behind the scenes.

> Only one service can listen on port `5432`. Stop a local Postgres server or another proxy using that port before creating a database.

## How it works

```text
Your app or database client
          │
          │  Postgres on localhost:5432
          ▼
    Neon Localhost
          │
          │  Authenticated Postgres over TLS
          ▼
      Neon Postgres
```

The local proxy binds only to the IPv4 and IPv6 loopback interfaces. Remote credentials remain inside the app and are never included in the local connection string.

## Develop locally

You will need Node.js, pnpm, Rust, and the macOS prerequisites for [Tauri](https://v2.tauri.app/start/prerequisites/).

```sh
pnpm install
pnpm tauri dev
```

The frontend is React and TypeScript. The native app and Postgres proxy are built with Rust and Tauri.

<details>
<summary><strong>Maintainer release process</strong></summary>

### Releasing

Create a version commit and annotated tag from a clean `main` branch:

```sh
pnpm release:patch # 0.1.0 -> 0.1.1
pnpm release:minor # 0.1.0 -> 0.2.0
pnpm release:major # 0.1.0 -> 1.0.0
git push origin main --follow-tags
```

The version tag triggers the macOS release workflow. It builds a universal Intel and Apple Silicon DMG, signs it with Developer ID, notarizes the app and DMG, verifies Gatekeeper acceptance, and creates a draft GitHub Release with a SHA-256 checksum. It also signs and publishes the in-app updater package and `latest.json`.

The release workflow requires these GitHub Actions secrets:

- `APPLE_CERTIFICATE` and `APPLE_CERTIFICATE_PASSWORD` for a base64-encoded Developer ID Application `.p12`;
- `APPLE_API_KEY_P8`, `APPLE_API_KEY_ID`, and `APPLE_API_ISSUER` for notarization;
- `APPLE_SIGNING_IDENTITY` and a generated `KEYCHAIN_PASSWORD` for the temporary CI keychain;
- `TAURI_SIGNING_PRIVATE_KEY` and `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` for cryptographically verified in-app updates.

The updater public key is embedded in the app. Keep a secure backup of the matching private key: existing installations cannot accept updates signed by a replacement key.

Release jobs create drafts intentionally. Test the downloaded DMG on a second Mac, then publish the draft from GitHub Releases.

</details>

---

Neon Localhost is an independent open-source project and is not affiliated with Neon.
