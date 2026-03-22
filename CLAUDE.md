# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make spec        # run all tests (verbose)
make fmt         # format source files
make lint        # run ameba linter
make fix         # auto-fix ameba issues
make all         # clean + fmt + lint + docs + spec

# Run a single spec file
crystal spec spec/document_spec.cr

# Run a single example by line number
crystal spec spec/document_spec.cr:42

# E2E tests (requires live CouchDB-compatible server)
make goydb       # start local server on :7070 (in separate terminal)
make e2e         # run HTTP adapter e2e tests
```

## Architecture

This is a Crystal shard providing a CouchDB client with both local (SQLite) and remote (HTTP) backends.

### Adapter Pattern

`Adapter::Base` defines ~23 abstract methods (CRUD, queries, replication primitives, attachments). Two implementations:

- **`Adapter::SQLite`** — local-first storage with full revision history, 4-table schema (`docs`, `revs`, `local_docs`, `update_seq`, `attachments`)
- **`Adapter::HTTP`** — proxies operations to a remote CouchDB server over HTTP/HTTPS with bearer token auth, TLS, and request/response interceptors

`Database` is the public facade that auto-detects which adapter to use based on location string (`http://...` → HTTP, `:memory:` → in-memory SQLite, anything else → file SQLite). It also accepts a pre-built adapter instance.

### Document Model

`Document` extends `JSON::Serializable::Unmapped` so unknown fields are preserved. Typed properties (`_id`, `_rev`, `_deleted`) coexist with arbitrary JSON fields accessible via hash syntax (`doc["key"]`). Users subclass `Document` for strongly-typed models.

### Replication

`Replication::Replicator` implements the 7-step CouchDB replication protocol between any two adapters (cross-adapter replication works: SQLite ↔ HTTP, SQLite ↔ SQLite, etc.). It is resumable via `Replication::Checkpoint`, which stores progress in `_local/` documents (never replicated). Checkpoint ID is derived from an MD5 hash of both database names.

Conflict resolution hooks (`on_conflict`, `on_remove_conflict`) are called during both normal writes and the replication write path.

### Error Hierarchy

`CouchDB::Error` → `NotFound`, `Conflict`, `Unauthorized`, `BadRequest`, `ReplicationError`
