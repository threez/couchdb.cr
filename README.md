# couchdb.cr

A Crystal shard for CouchDB — local-first storage backed by SQLite3 that replicates to and from a remote CouchDB server. Inspired by PouchDB.

Queries are answered instantly from local SQLite storage. Replication syncs with a remote CouchDB instance in the background, making it well-suited for offline-capable applications.

## Features

- **Local-first**: all reads and writes go to a local SQLite3 database — no network required
- **CouchDB replication**: 7-step protocol with resumable checkpoints; sync to/from any CouchDB 3.x server
- **Typed documents**: subclass `CouchDB::Document` to add strongly-typed fields to your models
- **Open schema**: unknown fields are preserved transparently through `json_unmapped`
- **Auto-routing**: pass a file path for SQLite, pass an `http://` URL for a remote CouchDB

## Installation

Add to your `shard.yml`:

```yaml
dependencies:
  couchdb:
    github: threez/couchdb.cr
    version: ~> 0.1
```

Then run:

```
shards install
```

## Quick Start

```crystal
require "couchdb"

db = CouchDB::Database.new("notes.db")

# Create a document
doc = CouchDB::Document.new
doc.id = "hello"
doc["message"] = JSON::Any.new("world")
result = db.put(doc)
puts result[:rev]   # => "1-5d41402abc4b2a76b9719d911017c592"

# Read it back
fetched = db.get("hello")
puts fetched["message"].as_s   # => "world"
puts fetched.rev               # => "1-5d41402abc4b2a76b9719d911017c592"

# Update — provide the current rev
fetched["message"] = JSON::Any.new("updated")
fetched.rev = result[:rev]
db.put(fetched)

# Delete
db.remove("hello", fetched.rev!)
```

## Typed Documents

Subclass `CouchDB::Document` to define strongly-typed models. Subclass fields are serialized as top-level JSON keys — they live alongside `_id`, `_rev`, and any other dynamic fields.

```crystal
class Note < CouchDB::Document
  property title : String = ""
  property body  : String = ""
  property tags  : Array(String) = [] of String
end

db = CouchDB::Database.new("notes.db")

note = Note.new
note.id    = "note-1"
note.title = "Shopping list"
note.body  = "Milk, eggs, bread"
db.put(note)

# Retrieve as the typed subclass
fetched = db.get("note-1", as: Note)
puts fetched.title   # => "Shopping list"
puts fetched.id      # => "note-1"
puts fetched.rev     # => "1-..."

# List all notes as typed objects
result = db.all_docs(as: Note)
result[:rows].each { |note| puts note.title }
```

Extra fields not declared on the subclass are still preserved in `json_unmapped` and round-trip through replication without loss.

## Document API

`CouchDB::Document` provides:

| Member | Type | Description |
|--------|------|-------------|
| `id` | `String` | Maps to `_id` in JSON |
| `rev` | `String?` | Maps to `_rev`; `nil` for new documents |
| `deleted` | `Bool?` | Maps to `_deleted`; `nil` for normal documents |
| `deleted?` | `Bool` | Predicate — returns `true` when `deleted == true` |
| `next_rev` | `String` | Computes what the next revision string would be |
| `json_unmapped` | `Hash(String, JSON::Any)` | All fields not covered by declared properties |
| `doc["key"]` | `JSON::Any` | Hash-style read (routes `_id`/`_rev`/`_deleted` to typed fields) |
| `doc["key"] = v` | — | Hash-style write |
| `doc["key"]?` | `JSON::Any?` | Hash-style read, returns `nil` if absent |

## Database API

```crystal
db = CouchDB::Database.new(location)
```

`location` is auto-detected:
- `"http://..."` or `"https://..."` → remote CouchDB via HTTP
- anything else → local SQLite (`.db` extension appended if not present; `":memory:"` for in-memory)

### CRUD

```crystal
db.get(id)                      # => Document   (raises NotFound)
db.get(id, as: MyDoc)           # => MyDoc       (typed subclass)
db.put(doc)                     # => {ok:, id:, rev:}  (raises Conflict)
db.remove(id, rev)              # => {ok:}
db.bulk_docs(docs)              # => [{id:, rev:, ok:}, ...]
db.bulk_docs(docs, new_edits: false)  # replication write path
```

### Query

```crystal
db.all_docs                                            # all non-deleted docs
db.all_docs(include_docs: true, limit: 50, skip: 0)
db.all_docs(startkey: "a", endkey: "m")               # range [a, m] inclusive
db.all_docs(startkey: "note-", endkey: "note-\uffff") # prefix scan
db.all_docs(as: Note)                                  # typed rows — implies include_docs: true
db.all_docs(as: Note, limit: 50, startkey: "note-")   # typed + range/pagination
db.changes(since: "0")                                 # changes feed (snapshot)
db.changes(since: seq, limit: 100, include_docs: true)
db.info                                                # => {db_name:, doc_count:, update_seq:}
```

### Changes Feed

`changes_feed` opens a persistent streaming connection, yielding each change to a block. Call `break` to stop.

```crystal
# Stream all changes from the beginning
db.changes_feed(since: "0") do |change|
  puts "#{change["id"]} changed (seq #{change["seq"]})"
  break if done?
end

# Pick up only changes after a known sequence, embedding full doc bodies
db.changes_feed(since: last_seq, include_docs: true) do |change|
  process(change["doc"])
  save_checkpoint(change["seq"].as_s)
  break if shutting_down?
end
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `since` | `"0"` | Starting sequence (exclusive). `"0"` yields all changes. |
| `heartbeat` | `1000` | Polling interval in ms (SQLite) or CouchDB heartbeat in ms (HTTP). |
| `include_docs` | `false` | Embed full document bodies in each change entry. |

**SQLite**: polls `update_seq` in a loop, sleeping `heartbeat` ms between polls.
**HTTP**: opens a `feed=continuous` connection to CouchDB and reads the response body line by line.

### Conflict Resolution

Register a hook on a `Database` instance to handle `put` or `remove` conflicts automatically instead of rescuing `Conflict` manually.

**`on_conflict`** — invoked when `put` raises `Conflict` (stale `_rev`):

```crystal
db.on_conflict do |existing, attempted|
  # existing  — the current document in the database (fresh rev)
  # attempted — the document you tried to write
  # Return a Document to retry with (rev is set automatically), or nil to re-raise.
  attempted   # last-write-wins
end
```

**`on_remove_conflict`** — invoked when `remove` raises `Conflict`:

```crystal
db.on_remove_conflict do |existing, attempted_rev|
  # existing      — the current document in the database
  # attempted_rev — the stale rev you passed to remove
  # Return true to retry the delete with the current rev, or nil to re-raise.
  true
end
```

Field-merge example:

```crystal
db.on_conflict do |existing, attempted|
  merged = CouchDB::Document.new
  merged.id = existing.id
  merged["count"] = JSON::Any.new(existing["count"].as_i + attempted["count"].as_i)
  merged
end
```

- A second conflict on retry propagates without re-invoking the hook (no infinite loop).
- Raise inside the hook to propagate a custom exception.
- Hooks apply to `put` and `remove` only; `bulk_docs` and attachment methods are unaffected.

### Attachments

```crystal
# Store a binary attachment (creates a new document revision)
rev = db.put(doc)[:rev]
db.put_attachment("doc-id", "photo.jpg", rev, bytes, "image/jpeg")  # => {ok:, id:, rev:}

# Retrieve raw bytes
att = db.get_attachment("doc-id", "photo.jpg")  # => {data: Bytes, content_type:}
File.write("photo.jpg", att[:data])

# Delete an attachment (creates a new document revision)
db.delete_attachment("doc-id", "photo.jpg", att_rev)  # => {ok:, id:, rev:}
```

Attachment metadata (content type, length) is stored as a stub in the document's `_attachments` field. Binary data is stored separately (in an `attachments` SQLite table for the local adapter, or via native CouchDB attachment endpoints for the HTTP adapter).

### Replication

```crystal
local  = CouchDB::Database.new("myapp.db")
remote = CouchDB::Database.new("https://admin:secret@db.example.com/myapp")

local.sync(remote)              # pull then push — full bidirectional sync
local.replicate_from(remote)    # pull only
local.replicate_to(remote)      # push only
```

`sync` and `replicate_*` return a `CouchDB::Replication::Session`:

```crystal
session = local.replicate_to(remote)
puts session.ok?             # true / false
puts session.docs_written    # number of documents transferred
puts session.docs_read       # number of documents fetched from source
puts session.last_seq        # last sequence number processed
puts session.error           # error message if ok == false
```

Replication is **resumable** — a checkpoint is written to both source and target after every batch of 100 documents, so interrupted replications restart from where they left off.

## Error Handling

```crystal
begin
  db.get("missing")
rescue CouchDB::NotFound => e
  puts e.message   # "Document not found: missing"
end

begin
  db.put(doc_with_wrong_rev)
rescue CouchDB::Conflict => e
  # Fetch the latest rev and retry
end
```

| Exception | When |
|-----------|------|
| `CouchDB::NotFound` | `get` on a non-existent or deleted document |
| `CouchDB::Conflict` | `put`/`remove` with a stale or missing revision |
| `CouchDB::Unauthorized` | HTTP 401 from remote CouchDB |
| `CouchDB::BadRequest` | Missing `_id`, missing `_rev` on replication write, etc. |
| `CouchDB::ReplicationError` | Unrecoverable failure during replication |

All exceptions inherit from `CouchDB::Error < Exception`.

## Architecture

```
CouchDB::Database           public facade — auto-detects adapter
        |
CouchDB::Adapter            abstract interface
  ├── Adapter::SQLite        local storage via crystal-db + crystal-sqlite3
  └── Adapter::HTTP          remote CouchDB via HTTP::Client
        |
CouchDB::Replication::Replicator   7-step CouchDB protocol
  ├── Replication::Checkpoint       _local/ checkpoint read/write
  └── Replication::Session          result object for one replication run
```

### SQLite schema

Four tables underpin the local adapter:

| Table | Purpose |
|-------|---------|
| `docs` | Every revision of every document (enables `revs_diff`) |
| `revs` | Parent-revision linkage tree |
| `local_docs` | `_local/` documents — checkpoints, never replicated |
| `update_seq` | Append-only sequence log; ROWID is the `update_seq` |
| `attachments` | Current binary attachment data keyed by `(doc_id, name)` |

The "winning" revision is the one with the highest `seq` for a given `id`. Deleted documents are soft-deleted (a `deleted=1` row is stored) so their revisions remain queryable for replication.

## Out of Scope (v0.1)

- Design documents and map-reduce views
- Filtered replication
- TLS client certificates

## Development

```bash
shards install
crystal spec          # all specs run without a CouchDB instance
```

To run e2e tests against a live server (optional):

```bash
# Option A: locally installed goydb
make goydb            # starts goydb on :7070 (foreground)
make e2e              # in another terminal

# Option B: Docker
docker run -d -p 7070:7070 ghcr.io/goydb/goydb:latest
make e2e
```

`COUCHDB_URL` defaults to `http://admin:secret@localhost:7070`. Override to point at any CouchDB-compatible server.

## Contributing

1. Fork it (<https://github.com/threez/couchdb.cr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Add specs for your change and make sure `crystal spec` passes
4. Commit your changes (`git commit -am 'Add some feature'`)
5. Push to the branch (`git push origin my-new-feature`)
6. Open a Pull Request

## Contributors

- [Vincent Landgraf](https://github.com/threez) — creator and maintainer
