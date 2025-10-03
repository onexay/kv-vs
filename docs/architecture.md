# Architecture Overview

The service exposes a REST API for versioning text blobs using KeyDB as the persistence engine. Each upload targets a repository (`name`) and branch, producing a Git-style commit hash plus diff information.

## Components
- **API Service**: Go HTTP server providing `/api/v1/blob` and `/api/v1/commits` endpoints. It validates requests and delegates versioning to the storage layer.
- **KeyDB Store**: Persists commits, branch heads, and blob contents. A simple in-memory store mirrors the interface for local development.

## Data Model (KeyDB)
- `commit:<repo>:<hash>` — JSON commit metadata (repo, branch, parent, content hash, timestamps).
- `content:<repo>:<hash>` — raw text payload for a commit.
- `branch:<repo>:<name>` — current commit hash for a branch.
- `branchset:<repo>` — set of branch names for listing.
- `tag:<repo>:<name>` — JSON metadata for a tag.
- `tagset:<repo>` — set of tag names.
- `repo:commits:<repo>` — sorted set of commit hashes (score = commit timestamp) for history queries.

## Write Path
1. Client issues `PUT /api/v1/blob/repo/<name>?branch=<branch>` with text content in the request body (headers supply author name/id).
2. Storage layer opens an optimistic transaction on the branch key, resolves the parent commit (if any), and loads prior content.
3. The new content is rehashed, a unified diff is generated (using `difflib`), and a commit hash is derived from repo, branch, parent, content, and timestamp.
4. Commit metadata, content, branch head, and history index entries are written atomically. The response returns the commit SHA, branch name, creation time, and diff.
5. After the write, retention logic checks the repository policy: older commits beyond the hot limit or duration are streamed into BoltDB and flagged as archived so only metadata remains hot.

## Read Path
- `GET /api/v1/commits?name=<repo>&order=desc&limit=20`: scans the repository history sorted set and hydrates commit metadata. Clients can request ascending order and trim results with `limit`.
- `GET /api/v1/branches?name=<repo>` / `POST /api/v1/branches?name=<repo>`: list or update branch pointers via JSON bodies.
- `GET /api/v1/tags?name=<repo>` / `POST /api/v1/tags?name=<repo>`: list or create lightweight tags anchored to commits.
- `GET /api/v1/policies?name=<repo>` / `POST /api/v1/policies`: query or set per-repository retention policies (immutable once set).
- `GET /swagger`: embedded Swagger UI for the REST contract.

Every `/api/v1` request must present `X-Author-Name` and `X-Author-ID` headers. The storage layer keeps a per-repository author registry; attempts to reuse an ID with a different name cause a conflict.
- `GET /api/v1/commits/{hash}?name=<repo>`: retrieves commit metadata and stored content for a specific revision.

## Configuration
- `STORAGE_BACKEND` selects `memory` (default) or `keydb`.
- `KEYDB_ADDR`, `KEYDB_USERNAME`, `KEYDB_PASSWORD`, `KEYDB_DB` configure the KeyDB client when enabled.
- `API_ADDR` overrides the HTTP bind address.

## Backup & Restore
- Mount KeyDB's data directory to a persistent volume (see `docker-compose.yml`).
- Schedule `keydb-cli --rdb /backups/kv-vs-$(date +%F).rdb` or rely on AOF snapshots for regular backups.
- To restore, place the RDB/AOF files back in `/data` and restart the KeyDB container.

## Deployment
- `docker-compose.yml` spins up the API and KeyDB services.
- Health probe: `GET /healthz`.
- Logs are plain-text JSON; plug in Prometheus/OpenTelemetry exporters as needed.
