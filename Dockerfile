# T3 Code - Remote Server
#
# Build:
#   docker build -t t3code .
#
# Run (mount your project directory):
#   docker run -it --rm \
#     -p 3773:3773 \
#     -v /path/to/your/project:/workspace \
#     -e T3CODE_AUTH_TOKEN="$(openssl rand -hex 24)" \
#     t3code
#
# The server starts in /workspace, which should be your project root.

# ── Build stage ──────────────────────────────────────────────────────
FROM oven/bun:1.3 AS build

WORKDIR /app

# Install system deps for native modules (node-pty)
RUN apt-get update && apt-get install -y \
  python3 \
  make \
  g++ \
  git \
  && rm -rf /var/lib/apt/lists/*

# Copy workspace config first for layer caching
COPY package.json bun.lock ./
COPY apps/server/package.json apps/server/
COPY apps/web/package.json apps/web/
COPY packages/contracts/package.json packages/contracts/
COPY packages/shared/package.json packages/shared/

# Install dependencies
RUN bun install --frozen-lockfile

# Copy source
COPY . .

# Build everything (contracts, shared, web, server)
RUN bun run build

# ── Runtime stage ────────────────────────────────────────────────────
FROM node:24-slim

WORKDIR /app

# Install runtime deps for node-pty and git (needed by the server)
RUN apt-get update && apt-get install -y \
  git \
  python3 \
  make \
  g++ \
  && rm -rf /var/lib/apt/lists/*

# Install Codex CLI globally (the agent backend)
RUN npm install -g @openai/codex

# Copy built server output
COPY --from=build /app/apps/server/dist /app/dist

# Copy built web client into the server dist (so it serves static files)
COPY --from=build /app/apps/web/dist /app/dist/client

# Copy node_modules for runtime deps (node-pty, ws, effect, etc.)
COPY --from=build /app/node_modules /app/node_modules
COPY --from=build /app/apps/server/node_modules /app/apps/server/node_modules
COPY --from=build /app/package.json /app/package.json
COPY --from=build /app/apps/server/package.json /app/apps/server/package.json

WORKDIR /workspace

EXPOSE 3773

# Default: listen on all interfaces, no auto-browser, with auth token from env
ENTRYPOINT ["node", "/app/dist/index.mjs", \
  "--host", "0.0.0.0", \
  "--port", "3773", \
  "--no-browser"]
