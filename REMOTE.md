# Remote Access Setup

Use this when you want to open T3 Code from another device (phone, tablet, another laptop).

## CLI / Env option map

The T3 Code CLI accepts the following configuration options, available either as CLI flags or environment variables:

| CLI flag                | Env var               | Notes                              |
| ----------------------- | --------------------- | ---------------------------------- |
| `--mode <web\|desktop>` | `T3CODE_MODE`         | Runtime mode.                      |
| `--port <number>`       | `T3CODE_PORT`         | HTTP/WebSocket port.               |
| `--host <address>`      | `T3CODE_HOST`         | Bind interface/address.            |
| `--state-dir <path>`    | `T3CODE_STATE_DIR`    | State directory.                   |
| `--dev-url <url>`       | `VITE_DEV_SERVER_URL` | Dev web URL redirect/proxy target. |
| `--no-browser`          | `T3CODE_NO_BROWSER`   | Disable auto-open browser.         |
| `--auth-token <token>`  | `T3CODE_AUTH_TOKEN`   | WebSocket auth token.              |

> TIP: Use the `--help` flag to see all available options and their descriptions.

## Security First

- Always set `--auth-token` before exposing the server outside localhost.
- Treat the token like a password.
- Prefer binding to trusted interfaces (LAN IP or Tailnet IP) instead of opening all interfaces unless needed.

## 1) Build + run server for remote access

Remote access should use the built web app (not local Vite redirect mode).

```bash
bun run build
TOKEN="$(openssl rand -hex 24)"
bun run --cwd apps/server start -- --host 0.0.0.0 --port 3773 --auth-token "$TOKEN" --no-browser
```

Then open on your phone using either method:

**Option A: Token in URL (one-click)**

```
http://<your-machine-ip>:3773/?token=<TOKEN>
```

The token is captured from the URL, stored in sessionStorage, and stripped from the address bar automatically. You stay authenticated for the browser session.

**Option B: Login prompt**

Navigate to `http://<your-machine-ip>:3773` without a token. The app shows a login screen where you paste the token.

Notes:

- `--host 0.0.0.0` listens on all IPv4 interfaces.
- `--no-browser` prevents local auto-open, which is usually better for headless/remote sessions.
- Ensure your OS firewall allows inbound TCP on the selected port.

## 2) Tailnet / Tailscale access

If you use Tailscale, you can bind directly to your Tailnet address.

```bash
TAILNET_IP="$(tailscale ip -4)"
TOKEN="$(openssl rand -hex 24)"
bun run --cwd apps/server start -- --host "$(tailscale ip -4)" --port 3773 --auth-token "$TOKEN" --no-browser
```

Open from any device in your tailnet:

```
http://<tailnet-ip>:3773/?token=<TOKEN>
```

You can also bind `--host 0.0.0.0` and connect through the Tailnet IP, but binding directly to the Tailnet IP limits exposure.

## 3) Docker

Build and run the containerized server:

```bash
docker build -t t3code .
TOKEN="$(openssl rand -hex 24)"

docker run -it --rm \
  -p 3773:3773 \
  -v /path/to/your/project:/workspace \
  -e T3CODE_AUTH_TOKEN="$TOKEN" \
  t3code
```

Open `http://<host>:3773/?token=<TOKEN>` from any device.

The container expects your project directory mounted at `/workspace`. The Codex CLI runs inside the container alongside the T3 Code server.

## 4) TLS / HTTPS

The server does not include built-in TLS. For any access over untrusted networks, put a TLS-terminating reverse proxy in front:

- **Caddy** (automatic HTTPS): `reverse_proxy localhost:3773`
- **Cloudflare Tunnel**: `cloudflared tunnel --url http://localhost:3773`
- **Tailscale HTTPS**: enable `tailscale serve` with TLS

The web client automatically uses `wss://` when the page is loaded over `https://`.
