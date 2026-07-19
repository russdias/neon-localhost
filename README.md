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
