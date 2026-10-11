# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow `shard.yml`'s own `version:` field (synced via `make version`, then
tagged via `make tag`).

## [Unreleased]

## [0.3.2] - 2026-10-11

- Upgraded to ameba 1.7.0 and standardized the CI pipeline (split lint/test
  jobs, pinned Crystal version, shards cache, kept the macOS and e2e jobs
  gated behind the lint job).
- ameba 1.7.0 no longer ships a `postinstall` build step for its bundled
  `shards install`, so `lib/ameba/bin/ameba` is now compiled explicitly from
  `lib/ameba/src/cli.cr` in both `make lint`/`make fix` and CI.
- Fixed `Adapter::SQLite#bulk_get` to inline attachment bytes (instead of
  returning a bare `stub: true` entry) when this adapter is used as a
  replication source, matching what `Adapter::HTTP#bulk_get` already did.
  Without this, replicating a document with an attachment from a local
  SQLite database to a target that had never seen that attachment's bytes
  (e.g. a real CouchDB server) failed with `412 missing_stub`; replicating
  to another SQLite database silently dropped the attachment data. See
  [#5](https://github.com/threez/couchdb.cr/issues/5). Covered by a new
  spec in `spec/replication_spec.cr`.
