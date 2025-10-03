# KeyDB Versioned Store (kv-vs)

A lightweight REST service that versions text blobs on top of KeyDB. Uploads are content-addressed, tracked per repository/branch, and produce Git-style commit hashes and diffs.

## Quick Start

```bash
# start the API locally (memory backend)
go run ./cmd/api
```

Set `STORAGE_BACKEND=keydb` plus `KEYDB_ADDR`, `KEYDB_USERNAME`, `KEYDB_PASSWORD`, and `KEYDB_DB` to use a KeyDB instance. Defaults fall back to the in-memory store.

## REST API

- `GET /healthz` — service heartbeat.
- `PUT /api/v1/blob/repo/<repo-name>?branch=<branch>` — upload text for a repository branch (branch defaults to `main`). The request body contains raw text; include headers `X-Author-Name` and `X-Author-ID` with per-repository unique author identifiers. Response matches:
  ```json
  {
    "commit": "8b7...",
    "branch": "experiment",
    "created_at": "2024-05-21T12:34:56Z",
    "diff": "--- previous\n+++ current\n..."
  }
  ```
- `GET /api/v1/blob/repo/<repo-name>?branch=<branch>&commit=<sha>` — fetch the latest (or specific) revision for a branch.
- `GET /api/v1/blob?name=<repo>&branch=<branch>` — fetch the latest commit content for a branch (defaults to `main`). Supply `commit=<sha>` to retrieve a specific revision.
- `GET /api/v1/commits?name=<repo>&order=desc&limit=20` — list commits for a repository. `order` accepts `asc`/`desc` (default `desc`). `limit` constrains the number of entries returned.
- `GET /api/v1/commits/{hash}?name=<repo>` — fetch commit metadata and the stored text for a given repository.
- `GET /api/v1/branches?name=<repo>` — list branches for a repository.
- `POST /api/v1/branches?name=<repo>` — create or move a branch pointer. Body `{"name":"dev","commit":"<sha>"}`.
- `GET /api/v1/branches/{branch}?name=<repo>` — retrieve branch metadata.
- `GET /api/v1/tags?name=<repo>` — list tags for a repository.
- `POST /api/v1/tags?name=<repo>` — create a tag pointing at a commit. Body `{"name":"v1.0.0","commit":"<sha>","note":"release"}`.
- `GET /api/v1/tags/{tag}?name=<repo>` — retrieve tag metadata.
- `POST /api/v1/policies` — set a repository’s retention policy (immutable per repo). Body `{"name":"analytics","hotCommitLimit":50,"hotDuration":"168h"}`.
- `GET /api/v1/policies?name=<repo>` — fetch the effective retention policy for a repository.
- `GET /swagger` — embedded Swagger UI backed by the bundled OpenAPI document.

All `/api/v1` requests must include `X-Author-Name` and `X-Author-ID` headers. Author IDs are enforced to be unique per repository; reusing an ID with a different name is rejected.

## Versioning Flow

1. Clients submit raw text to `PUT /api/v1/blob/repo/<name>` with `X-Author-Name` / `X-Author-ID` headers (unique per repo).
   ```bash
   curl -X PUT "http://localhost:8080/api/v1/blob/repo/analytics?branch=experiment" \
     -H 'X-Author-Name: Alice Analyst' \
     -H 'X-Author-ID: alice@example.com' \
     --data-binary @data.csv
   ```
2. The service rehashes input, detects the previous head for the target branch (default `main`), computes a unified diff, and writes a new commit.
3. The response contains the commit SHA, effective branch name, creation timestamp, and the diff against the previous revision.
4. Repositories maintain independent branch pointers, so parallel branches can be updated by supplying the `branch` query parameter.

## Container Setup

```bash
docker compose up --build
```

The Compose stack provisions:
- `api`: Go REST service compiled into a distroless image.
- `keydb`: KeyDB server with AOF persistence and a named volume `keydb-data` for durable history.

## Development Notes

- Run storage tests with `go test ./internal/storage`; they depend on loopback sockets (Miniredis) when exercising the KeyDB backend.
- Future work: add cursor-based pagination and retention policies for large repositories.

### Retention Configuration

Set the following environment variables (or edit `configs/default.yaml`) to control the hybrid hot/cold blob cache:

- `RETENTION_ARCHIVE_PATH` — BoltDB file used for archived blobs (`data/archive.db` by default).
- `RETENTION_HOT_COMMIT_LIMIT` — maximum number of recent commits kept in memory per repository (0 = unlimited).
- `RETENTION_HOT_DURATION` — `time.ParseDuration` string (e.g., `168h`) specifying how long commits stay hot; archives anything older.

### Admin CLI

A small admin utility lives under `cmd/admin` for quick policy inspection:

```bash
  go build -o bin/kvvs-admin ./cmd/admin

  # Show policy for repo "analytics" (tabular)
  ./bin/kvvs-admin --repo analytics --api http://localhost:8080

# JSON output, API base can also be set via KVVS_API
KVVS_API=http://staging:8080 ./bin/kvvs-admin --repo analytics --json
```

### Swagger UI

The embedded OpenAPI document and Swagger UI are available at `http://localhost:8080/swagger`. The UI serves the bundled `docs/openapi.yaml`, so no additional tooling is required.
