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
  # ```
  # db = CouchDB::Database.new("notes.db")                            # local SQLite
  # db = CouchDB::Database.new(":memory:")                            # in-memory SQLite (testing)
  # db = CouchDB::Database.new("http://admin:pw@localhost:5984/mydb") # remote CouchDB
  # ```
  class Database
    include Adapter

    getter adapter : Adapter

    # Resolver for `put` conflicts: receives (existing, attempted), returns a Document
    # to write (system stamps the current rev) or nil to re-raise.
    alias PutConflictResolver = Proc(Document, Document, Document?)

    # Resolver for `remove` conflicts: receives (existing, attempted_rev), returns
    # true to retry the delete with the current rev, or nil to re-raise.
    alias RemoveConflictResolver = Proc(Document, String, Bool?)

    @put_conflict_resolver : PutConflictResolver? = nil
    @remove_conflict_resolver : RemoveConflictResolver? = nil

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
    # ```
    # note = db.get("note-1", as: MyNote)
    # note.title # strongly-typed field
    # ```
    def get(id : String, as klass : T.class) : T forall T
      klass.from_json(@adapter.get(id).to_json)
    end

    # Register a block invoked when `put` raises Conflict.
    # Return a Document to retry (system sets the correct rev automatically).
    # Return nil to re-raise. Raise to propagate a custom exception.
    def on_conflict(&block : Document, Document -> Document?)
      @put_conflict_resolver = block
    end

    # Register a block invoked when `remove` raises Conflict.
    # Return true to retry the delete with the current rev. Return nil to re-raise.
    def on_remove_conflict(&block : Document, String -> Bool?)
      @remove_conflict_resolver = block
    end

    # Creates or updates a document. Pass `doc.rev` for updates. Raises `Conflict` on
    # a rev mismatch. Returns `{ok: true, id:, rev:}`.
    def put(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
      @adapter.put(doc)
    rescue conflict : Conflict
      resolver = @put_conflict_resolver
      raise conflict unless resolver

      existing = @adapter.get(doc.id)
      resolved = resolver.call(existing, doc)
      raise conflict unless resolved

      resolved.rev = existing.rev
      @adapter.put(resolved)
    end

    # Soft-deletes a document by writing a tombstone. Raises `Conflict` on rev mismatch.
    def remove(id : String, rev : String) : NamedTuple(ok: Bool)
      @adapter.remove(id, rev)
    rescue conflict : Conflict
      resolver = @remove_conflict_resolver
      raise conflict unless resolver

      existing = @adapter.get(id)
      result = resolver.call(existing, rev)
      raise conflict unless result

      @adapter.remove(id, existing.rev || "")
    end

    # Batch write. Pass `new_edits: false` for the replication write path (bypasses
    # conflict detection and stores revisions exactly as supplied).
    def bulk_docs(docs : Array(Document), new_edits : Bool = true) : Array(NamedTuple(id: String, rev: String, ok: Bool))
      @adapter.bulk_docs(docs, new_edits)
    end

    # Lists all non-deleted documents. Pass `include_docs: true` for full bodies,
    # `limit`/`skip` for pagination, `startkey`/`endkey` for range queries (inclusive).
    def all_docs(include_docs : Bool = false, limit : Int32? = nil, skip : Int32 = 0,
                 startkey : String? = nil, endkey : String? = nil) : NamedTuple(
      total_rows: Int64,
      offset: Int32,
      rows: Array(JSON::Any))
      @adapter.all_docs(include_docs, limit, skip, startkey, endkey)
    end

    # Typed overload — deserializes each row's `doc` into *klass*.
    # Implies `include_docs: true`; all other parameters work identically.
    #
    # Example:
    # ```
    # result = db.all_docs(as: Note, limit: 50)
    # result[:rows] # => Array(Note)
    # result[:total_rows]
    # ```
    def all_docs(as klass : T.class, limit : Int32? = nil, skip : Int32 = 0,
                 startkey : String? = nil, endkey : String? = nil) : NamedTuple(
      total_rows: Int64,
      offset: Int32,
      rows: Array(T)) forall T
      raw = @adapter.all_docs(true, limit, skip, startkey, endkey)
      typed_rows = raw[:rows].map { |row| klass.from_json(row["doc"].to_json) }
      {total_rows: raw[:total_rows], offset: raw[:offset], rows: typed_rows}
    end

    # Returns changes since a sequence number. Use `"0"` to fetch all changes.
    # Pass `include_docs: true` to embed full document bodies in each change entry.
    def changes(since : String = "0", limit : Int32? = nil, include_docs : Bool = false) : NamedTuple(
      last_seq: String,
      results: Array(JSON::Any))
      @adapter.changes(since, limit, include_docs)
    end

    # Streams changes continuously, yielding each change entry to the block.
    # Call `break` from the block to stop. `since` defaults to `"0"` (all changes).
    # `heartbeat` controls polling interval (SQLite) or CouchDB heartbeat (HTTP) in ms.
    def changes_feed(since : String = "0", heartbeat : Int32 = 1000,
                     include_docs : Bool = false, &block : JSON::Any -> _)
      @adapter.changes_feed(since, heartbeat, include_docs, &block)
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

    # Returns raw bytes and content-type for an attachment. Raises `NotFound` if absent.
    def get_attachment(id : String, attname : String) : NamedTuple(data: Bytes, content_type: String)
      @adapter.get_attachment(id, attname)
    end

    # Stores binary data as an attachment, creating a new document revision.
    # Raises `Conflict` on rev mismatch.
    def put_attachment(id : String, attname : String, rev : String,
                       data : Bytes, content_type : String) : NamedTuple(ok: Bool, id: String, rev: String)
      @adapter.put_attachment(id, attname, rev, data, content_type)
    end

    # Removes an attachment, creating a new document revision.
    # Raises `Conflict` on rev mismatch.
    def delete_attachment(id : String, attname : String, rev : String) : NamedTuple(ok: Bool, id: String, rev: String)
      @adapter.delete_attachment(id, attname, rev)
    end

    # Runs an in-memory map/reduce query over all documents, PouchDB-style.
    #
    # The *map* block receives each document and an `emit` proc; call `emit.call(key, value)`
    # to include a row in the result set. Rows are sorted by *key* using CouchDB collation order.
    #
    # ```
    # result = db.query do |doc, emit|
    #   emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(1_i64))
    # end
    # result[:rows].each { |r| puts "#{r["key"]} → #{r["value"]}" }
    # ```
    def query(
      key : JSON::Any? = nil,
      keys : Array(JSON::Any)? = nil,
      startkey : JSON::Any? = nil,
      endkey : JSON::Any? = nil,
      limit : Int32? = nil,
      skip : Int32 = 0,
      descending : Bool = false,
      include_docs : Bool = false,
      reduce : String? = nil,
      group : Bool = false,
      group_level : Int32? = nil,
      &map : Document, Proc(JSON::Any?, JSON::Any?, Nil) -> _
    ) : NamedTuple(total_rows: Int64, offset: Int32, rows: Array(JSON::Any))
      # 1. Map phase: page through all docs in batches to avoid loading everything at once.
      # doc_lookup is only populated when include_docs is true (needed for output row building).
      emitted = [] of EmittedRow
      doc_lookup = {} of String => JSON::Any
      scan_offset = 0
      loop do
        batch = @adapter.all_docs(true, QUERY_BATCH_SIZE, scan_offset, nil, nil)
        break if batch[:rows].empty?
        batch[:rows].each do |row|
          doc_id = row["id"].as_s
          doc_json = row["doc"]
          doc_lookup[doc_id] = doc_json if include_docs
          doc = Document.from_json(doc_json.to_json)
          emit_fn = ->(k : JSON::Any?, v : JSON::Any?) {
            emitted << EmittedRow.new(doc_id: doc_id, key: k, value: v)
            nil
          }
          map.call(doc, emit_fn)
        end
        break if batch[:rows].size < QUERY_BATCH_SIZE
        scan_offset += QUERY_BATCH_SIZE
      end

      # 4. Sort by CouchDB collation order
      emitted.sort! { |a, b| compare_json_keys(a.key, b.key) }

      # 5. Descending: reverse + swap key bounds for filtering
      eff_start = startkey
      eff_end = endkey
      if descending
        emitted.reverse!
        eff_start, eff_end = endkey, startkey
      end

      # 6. Filter
      filtered = emitted.select { |r| query_key_matches?(r.key, key, keys, eff_start, eff_end) }
      total_rows = filtered.size.to_i64

      # 7. Reduce path (skip/limit not applied to reduce results)
      if (fn = reduce)
        return {total_rows: total_rows, offset: 0,
                rows: apply_reduce(fn, filtered, group || !group_level.nil?, group_level)}
      end

      # 8. Paginate
      paged = filtered[skip..]? || [] of EmittedRow
      paged = paged.first(limit) if limit

      # 9. Build output rows
      rows = paged.map do |r|
        entry = {
          "id"    => JSON::Any.new(r.doc_id),
          "key"   => r.key || JSON::Any.new(nil),
          "value" => r.value || JSON::Any.new(nil),
        } of String => JSON::Any
        entry["doc"] = doc_lookup[r.doc_id] if include_docs
        JSON::Any.new(entry)
      end

      {total_rows: total_rows, offset: skip, rows: rows}
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

    QUERY_BATCH_SIZE = 1000

    private record EmittedRow, doc_id : String, key : JSON::Any?, value : JSON::Any?

    private def type_order(v : JSON::Any?) : Int32
      return 0 if v.nil?
      case v.raw
      when Nil            then 0
      when Bool           then v.as_bool ? 2 : 1
      when Int64, Float64 then 3
      when String         then 4
      when Array          then 5
      when Hash           then 6
      else                     0
      end
    end

    private def compare_json_keys(a : JSON::Any?, b : JSON::Any?) : Int32
      ta, tb = type_order(a), type_order(b)
      return ta <=> tb unless ta == tb
      case ta
      when 0, 1, 2
        0 # null / false / true: same rank means value equality
      when 3
        (a.not_nil!.raw.as(Int64 | Float64).to_f64 <=> b.not_nil!.raw.as(Int64 | Float64).to_f64) || 0
      when 4
        (a.not_nil!.as_s <=> b.not_nil!.as_s) || 0
      when 5
        arr_a, arr_b = a.not_nil!.as_a, b.not_nil!.as_a
        arr_a.each_with_index do |elem, i|
          return 1 if i >= arr_b.size
          cmp = compare_json_keys(elem, arr_b[i])
          return cmp unless cmp == 0
        end
        (arr_a.size <=> arr_b.size) || 0
      else
        (a.not_nil!.to_json <=> b.not_nil!.to_json) || 0
      end
    end

    private def query_key_matches?(
      k : JSON::Any?,
      exact : JSON::Any?, exact_set : Array(JSON::Any)?,
      startkey : JSON::Any?, endkey : JSON::Any?,
    ) : Bool
      if exact_set
        exact_set.any? { |e| compare_json_keys(k, e) == 0 }
      elsif exact
        compare_json_keys(k, exact) == 0
      else
        (startkey.nil? || compare_json_keys(k, startkey) >= 0) &&
          (endkey.nil? || compare_json_keys(k, endkey) <= 0)
      end
    end

    private def group_key_for(key : JSON::Any?, level : Int32?) : JSON::Any?
      return key if level.nil? || key.nil?
      return key unless key.raw.is_a?(Array)
      JSON::Any.new(key.as_a.first(level))
    end

    private def reduce_values(fn : String, values : Array(JSON::Any?)) : JSON::Any
      case fn
      when "_count"
        JSON::Any.new(values.size.to_i64)
      when "_sum"
        total = values.sum do |v|
          raise ArgumentError.new("_sum requires numeric values, got: #{v.inspect}") unless v && (v.raw.is_a?(Int64) || v.raw.is_a?(Float64))
          v.raw.as(Int64 | Float64).to_f64
        end
        t = total.to_i64
        t.to_f64 == total ? JSON::Any.new(t) : JSON::Any.new(total)
      when "_stats"
        nums = values.map do |v|
          raise ArgumentError.new("_stats requires numeric values, got: #{v.inspect}") unless v && (v.raw.is_a?(Int64) || v.raw.is_a?(Float64))
          v.raw.as(Int64 | Float64).to_f64
        end
        JSON::Any.new({
          "sum"   => JSON::Any.new(nums.sum),
          "count" => JSON::Any.new(nums.size.to_i64),
          "min"   => JSON::Any.new(nums.min),
          "max"   => JSON::Any.new(nums.max),
          "sumsq" => JSON::Any.new(nums.sum { |x| x * x }),
        } of String => JSON::Any)
      else
        raise ArgumentError.new("Unknown reduce function: #{fn}")
      end
    end

    private def apply_reduce(
      fn : String, rows : Array(EmittedRow),
      do_group : Bool, group_level : Int32?,
    ) : Array(JSON::Any)
      return [] of JSON::Any if rows.empty?
      unless do_group
        return [JSON::Any.new({
          "key"   => JSON::Any.new(nil),
          "value" => reduce_values(fn, rows.map(&.value)),
        } of String => JSON::Any)]
      end
      groups = Hash(String, Array(EmittedRow)).new { |h, k| h[k] = [] of EmittedRow }
      rows.each { |r| groups[group_key_for(r.key, group_level).to_json] << r }
      groups.map do |_, g|
        gkey = group_key_for(g.first.key, group_level)
        JSON::Any.new({
          "key"   => gkey || JSON::Any.new(nil),
          "value" => reduce_values(fn, g.map(&.value)),
        } of String => JSON::Any)
      end
    end
  end
end
