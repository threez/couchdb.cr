require "./adapter/base"
require "./adapter/sqlite"
require "./adapter/http"
require "./replication/replicator"

module CouchDB
  # Public facade for CouchDB database operations.
  #
  # `Database` auto-detects the appropriate adapter from the *location* string and
  # delegates all operations to it. It implements the `Adapter` interface so it
  # can be passed anywhere an adapter is accepted.
  #
  # ```crystal
  # db = CouchDB::Database.new("notes.db")          # local SQLite
  # db = CouchDB::Database.new(":memory:")           # in-memory SQLite (testing)
  # db = CouchDB::Database.new("http://admin:pw@localhost:5984/mydb")  # remote CouchDB
  # ```
  class Database
    include Adapter

    getter adapter : Adapter

    # Creates a new database, auto-selecting the adapter based on *location*.
    #
    # - `"http://..."` or `"https://..."` → `Adapter::HTTP` (remote CouchDB)
    # - `":memory:"` → `Adapter::SQLite` in-memory (no persistence)
    # - any other string → `Adapter::SQLite` file database (`.db` appended if needed)
    def initialize(location : String)
      @adapter = if location.starts_with?("http://") || location.starts_with?("https://")
        Adapter::HTTP.new(location)
      else
        path = (location == ":memory:" || location.ends_with?(".db")) ? location : "#{location}.db"
        Adapter::SQLite.new(path)
      end
    end

    # Returns `{db_name:, doc_count:, update_seq:}` for the underlying database.
    def info : NamedTuple(db_name: String, doc_count: Int64, update_seq: Int64)
      @adapter.info
    end

    # Fetches the winning revision of a document. Raises `NotFound` if absent or deleted.
    def get(id : String) : Document
      @adapter.get(id)
    end

    # Typed overload — deserializes the stored document into a specific `Document` subclass.
    #
    # Example:
    # ```crystal
    # note = db.get("note-1", as: MyNote)
    # note.title  # strongly-typed field
    # ```
    def get(id : String, as klass : T.class) : T forall T
      klass.from_json(@adapter.get(id).to_json)
    end

    # Creates or updates a document. Pass `doc.rev` for updates. Raises `Conflict` on
    # a rev mismatch. Returns `{ok: true, id:, rev:}`.
    def put(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
      @adapter.put(doc)
    end

    # Soft-deletes a document by writing a tombstone. Raises `Conflict` on rev mismatch.
    def remove(id : String, rev : String) : NamedTuple(ok: Bool)
      @adapter.remove(id, rev)
    end

    # Batch write. Pass `new_edits: false` for the replication write path (bypasses
    # conflict detection and stores revisions exactly as supplied).
    def bulk_docs(docs : Array(Document), new_edits : Bool = true) : Array(NamedTuple(id: String, rev: String, ok: Bool))
      @adapter.bulk_docs(docs, new_edits)
    end

    # Lists all non-deleted documents. Pass `include_docs: true` for full bodies,
    # `limit`/`skip` for pagination.
    def all_docs(include_docs : Bool = false, limit : Int32? = nil, skip : Int32 = 0) : NamedTuple(
      total_rows: Int64,
      offset: Int32,
      rows: Array(JSON::Any)
    )
      @adapter.all_docs(include_docs, limit, skip)
    end

    # Returns changes since a sequence number. Use `"0"` to fetch all changes.
    # Pass `include_docs: true` to embed full document bodies in each change entry.
    def changes(since : String = "0", limit : Int32? = nil, include_docs : Bool = false) : NamedTuple(
      last_seq: String,
      results: Array(JSON::Any)
    )
      @adapter.changes(since, limit, include_docs)
    end

    # Returns missing revisions for the given `id → [revs]` map.
    # Used internally by the replication engine.
    def revs_diff(id_revs : Hash(String, Array(String))) : Hash(String, NamedTuple(missing: Array(String)))
      @adapter.revs_diff(id_revs)
    end

    # Fetches specific `{id, rev}` pairs in bulk.
    # Used internally by the replication engine.
    def bulk_get(id_revs : Array(NamedTuple(id: String, rev: String))) : Array(Document)
      @adapter.bulk_get(id_revs)
    end

    # Reads a `_local/` checkpoint document. Used internally by replication.
    def get_local(id : String) : Document
      @adapter.get_local(id)
    end

    # Writes a `_local/` checkpoint document. Used internally by replication.
    def put_local(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
      @adapter.put_local(doc)
    end

    # Pushes local changes to *target*. Returns a `Replication::Session` with transfer stats.
    def replicate_to(target : Database) : Replication::Session
      Replication::Replicator.new(@adapter, target.adapter).replicate
    end

    # Pulls changes from *source* into this database. Returns a `Replication::Session`.
    def replicate_from(source : Database) : Replication::Session
      Replication::Replicator.new(source.adapter, @adapter).replicate
    end

    # Bidirectional sync: pulls from *remote* then pushes to *remote*.
    # Equivalent to `replicate_from(remote)` followed by `replicate_to(remote)`.
    def sync(remote : Database)
      replicate_from(remote)
      replicate_to(remote)
    end
  end
end
