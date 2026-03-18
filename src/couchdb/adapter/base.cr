require "../document"
require "../error"

module CouchDB
  # Abstract interface implemented by both `Adapter::SQLite` and `Adapter::HTTP`.
  #
  # Users interact with `CouchDB::Database`, which wraps an adapter and delegates
  # all operations to it. Adapters are not instantiated directly in normal usage.
  module Adapter
    # Returns database metadata: name, live document count, and latest update sequence.
    abstract def info : NamedTuple(db_name: String, doc_count: Int64, update_seq: Int64)

    # Fetches the winning revision of a document by ID.
    # Raises `NotFound` if the document is absent or has been deleted.
    abstract def get(id : String) : Document

    # Creates or updates a document.
    #
    # For updates, `doc.rev` must match the current winning revision; raises `Conflict`
    # on a mismatch. For new documents, `doc.rev` must be `nil` or empty.
    # Returns `{ok: true, id:, rev:}` on success.
    abstract def put(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)

    # Soft-deletes a document by writing a tombstone revision.
    # Raises `Conflict` when `rev` does not match the current winning revision.
    abstract def remove(id : String, rev : String) : NamedTuple(ok: Bool)

    # Writes multiple documents in a single operation.
    #
    # When `new_edits` is `true` (default), normal conflict detection applies and
    # new revisions are generated. When `new_edits` is `false`, documents are stored
    # with their supplied revision strings exactly as given — this is the replication
    # write path and bypasses conflict checking.
    abstract def bulk_docs(docs : Array(Document), new_edits : Bool) : Array(NamedTuple(id: String, rev: String, ok: Bool))

    # Returns all non-deleted winning revisions, sorted by `_id`.
    #
    # Pass `include_docs: true` to embed full document bodies in each row's `"doc"` field.
    # Use `limit` and `skip` for pagination.
    abstract def all_docs(include_docs : Bool, limit : Int32?, skip : Int32) : NamedTuple(
      total_rows: Int64,
      offset: Int32,
      rows: Array(JSON::Any))

    # Returns the changes feed since a given sequence number.
    #
    # `since` is the last sequence number already seen (use `"0"` to fetch all changes).
    # Pass `include_docs: true` to embed full document bodies in each change entry.
    abstract def changes(since : String, limit : Int32?, include_docs : Bool) : NamedTuple(
      last_seq: String,
      results: Array(JSON::Any))

    # Given a map of `id → [revs]`, returns only the revisions missing from this adapter.
    #
    # Used by the replication engine to determine which documents need to be transferred.
    # Returns an empty hash when all supplied revisions are already present.
    abstract def revs_diff(id_revs : Hash(String, Array(String))) : Hash(String, NamedTuple(missing: Array(String)))

    # Fetches specific `{id, rev}` pairs in bulk.
    #
    # Returns only documents that were found; missing pairs are silently skipped.
    # Used internally by the replication engine after `revs_diff`.
    abstract def bulk_get(id_revs : Array(NamedTuple(id: String, rev: String))) : Array(Document)

    # Reads a `_local/` checkpoint document by ID.
    #
    # `_local/` documents are never replicated. Raises `NotFound` if absent.
    abstract def get_local(id : String) : Document

    # Writes a `_local/` checkpoint document, creating or replacing it.
    #
    # The `_local/` prefix is added automatically if not already present.
    abstract def put_local(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
  end
end
