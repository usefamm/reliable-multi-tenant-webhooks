# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Single image, five processes. The API, both workers, the mock receiver and the
# one-shot schema bootstrap are all entrypoints of this image: they are the same
# codebase, and running them from one image is what makes "two independent worker
# processes" a deployment fact rather than a claim.
# ---------------------------------------------------------------------------

# ---- deps ------------------------------------------------------------------
# Install from the lockfile (`npm ci`, never `npm install`: the lockfile is the
# build's source of truth). Split into its own layer so a source-only change does
# not reinstall dependencies.
FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

# ---- build -----------------------------------------------------------------
FROM node:20-alpine AS build
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# Typecheck the whole project (including tests) before emitting, then compile.
RUN npm run typecheck && npm run build
# Drop the toolchain from the artefact: the runtime needs production deps only.
RUN npm ci --omit=dev

# ---- runtime ---------------------------------------------------------------
FROM node:20-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
# No .env is copied (see .dockerignore): configuration arrives from the
# environment, so no credential is ever baked into an image layer.
COPY --from=build --chown=node:node /app/package.json ./package.json
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
# Migrations are read at runtime by dist/db/migrate.js (`__dirname/../../migrations`).
COPY --from=build --chown=node:node /app/migrations ./migrations
USER node
EXPOSE 3000 4000
CMD ["node", "dist/api/main.js"]
