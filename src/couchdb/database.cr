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

    @adapter : Adapter::HTTP | Adapter::SQLite

    def adapter : Adapter
      @adapter
    end

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

    # Assigns a TLS client context used for mutual TLS (mTLS) on HTTPS connections.
    # No-op when the underlying adapter is SQLite.
    #
    # ```
    # ctx = OpenSSL::SSL::Context::Client.new
    # ctx.certificate_file = "client.crt"
    # ctx.private_key_file = "client.key"
    # db.tls = ctx
    # ```
    def tls=(ctx : OpenSSL::SSL::Context::Client)
      case a = @adapter
      when Adapter::HTTP then a.tls = ctx
      end
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
    rescue ex : Conflict
      resolver = @put_conflict_resolver
      raise ex unless resolver

      existing = @adapter.get(doc.id)
      resolved = resolver.call(existing, doc)
      raise ex unless resolved

      resolved.rev = existing.rev
      @adapter.put(resolved)
    end

    # Soft-deletes a document by writing a tombstone. Raises `Conflict` on rev mismatch.
    def remove(id : String, rev : String) : NamedTuple(ok: Bool)
      @adapter.remove(id, rev)
    rescue ex : Conflict
      resolver = @remove_conflict_resolver
      raise ex unless resolver

      existing = @adapter.get(id)
      result = resolver.call(existing, rev)
      raise ex unless result

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
                     include_docs : Bool = false, & : JSON::Any -> _)
      @adapter.changes_feed(since, heartbeat, include_docs) { |feed_entry| yield feed_entry }
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
      emitted.sort! { |lhs, rhs| compare_json_keys(lhs.key, rhs.key) }

      # 5. Descending: reverse + swap key bounds for filtering
      eff_start = startkey
      eff_end = endkey
      if descending
        emitted.reverse!
        eff_start, eff_end = endkey, startkey
      end

      # 6. Filter
      filtered = emitted.select { |emitted_row| query_key_matches?(emitted_row.key, key, keys, eff_start, eff_end) }
      total_rows = filtered.size.to_i64

      # 7. Reduce path (skip/limit not applied to reduce results)
      if fn = reduce
        return {total_rows: total_rows, offset: 0,
                rows: apply_reduce(fn, filtered, group || !group_level.nil?, group_level)}
      end

      # 8. Paginate
      paged = filtered[skip..]? || [] of EmittedRow
      paged = paged.first(limit) if limit

      # 9. Build output rows
      rows = build_query_rows(paged, include_docs, doc_lookup)

      {total_rows: total_rows, offset: skip, rows: rows}
    end

    # Runs an in-memory Mango selector query over all documents, PouchDB/CouchDB-style.
    #
    # *selector* is a JSON::Any hash describing match conditions (see README for full operator
    # reference). *fields* limits the keys returned per doc. *sort* is an array of bare field
    # name strings (ascending) or single-key hashes with `"asc"`/`"desc"` values. *limit* and
    # *skip* control pagination.
    #
    # ```
    # result = db.find(JSON.parse(%({"type": "note"})))
    # result[:docs].each { |doc| puts doc["title"] }
    # result[:warning] # => always present (full scan, no index)
    # ```
    def find(
      selector : JSON::Any,
      fields : Array(String)? = nil,
      sort : Array(JSON::Any)? = nil,
      limit : Int32? = nil,
      skip : Int32 = 0,
    ) : NamedTuple(docs: Array(JSON::Any), warning: String?)
      docs = [] of JSON::Any
      scan_offset = 0
      loop do
        batch = @adapter.all_docs(true, QUERY_BATCH_SIZE, scan_offset, nil, nil)
        break if batch[:rows].empty?
        batch[:rows].each do |row|
          doc_json = row["doc"]
          docs << doc_json if match_selector?(doc_json, selector)
        end
        break if batch[:rows].size < QUERY_BATCH_SIZE
        scan_offset += QUERY_BATCH_SIZE
      end

      docs = apply_find_sort(docs, sort) if sort && !sort.empty?

      paged = docs[skip..]? || [] of JSON::Any
      paged = paged.first(limit) if limit

      result_docs = if f = fields
                      paged.map { |doc| project_doc_fields(doc, f).as(JSON::Any) }
                    else
                      paged
                    end

      {docs: result_docs, warning: "no matching index found, create an index to optimize query time"}
    end

    # Pushes local changes to *target*. Returns a `Replication::Session` with transfer stats.
    #
    # Pass *doc_ids* to replicate only specific documents by ID.
    # Pass *selector* (a Mango selector as `JSON::Any`) to filter by document content.
    # Pass *filter* for an ad-hoc `Proc(Document, Bool)` predicate.
    # *selector* and *filter* are mutually exclusive; *filter* takes precedence.
    def replicate_to(
      target : Database,
      doc_ids : Array(String)? = nil,
      selector : JSON::Any? = nil,
      filter : Proc(Document, Bool)? = nil,
    ) : Replication::Session
      proc = filter || selector_to_filter(selector)
      Replication::Replicator.new(@adapter, target.adapter, doc_ids, proc).replicate
    end

    # Pulls changes from *source* into this database. Returns a `Replication::Session`.
    #
    # Pass *doc_ids* to replicate only specific documents by ID.
    # Pass *selector* (a Mango selector as `JSON::Any`) to filter by document content.
    # Pass *filter* for an ad-hoc `Proc(Document, Bool)` predicate.
    def replicate_from(
      source : Database,
      doc_ids : Array(String)? = nil,
      selector : JSON::Any? = nil,
      filter : Proc(Document, Bool)? = nil,
    ) : Replication::Session
      proc = filter || selector_to_filter(selector)
      Replication::Replicator.new(source.adapter, @adapter, doc_ids, proc).replicate
    end

    # Bidirectional sync: pulls from *remote* then pushes to *remote*.
    # Equivalent to `replicate_from(remote)` followed by `replicate_to(remote)`.
    # Accepts the same *doc_ids*, *selector*, and *filter* options as `replicate_to`/`replicate_from`.
    def sync(
      remote : Database,
      doc_ids : Array(String)? = nil,
      selector : JSON::Any? = nil,
      filter : Proc(Document, Bool)? = nil,
    )
      replicate_from(remote, doc_ids: doc_ids, selector: selector, filter: filter)
      replicate_to(remote, doc_ids: doc_ids, selector: selector, filter: filter)
    end

    private def selector_to_filter(sel : JSON::Any?) : Proc(Document, Bool)?
      return nil unless sel
      ->(doc : Document) { match_selector?(JSON.parse(doc.to_json), sel) }
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
        (a.as(JSON::Any).raw.as(Int64 | Float64).to_f64 <=> b.as(JSON::Any).raw.as(Int64 | Float64).to_f64) || 0
      when 4
        (a.as(JSON::Any).as_s <=> b.as(JSON::Any).as_s) || 0
      when 5
        arr_a, arr_b = a.as(JSON::Any).as_a, b.as(JSON::Any).as_a
        arr_a.each_with_index do |elem, i|
          return 1 if i >= arr_b.size
          cmp = compare_json_keys(elem, arr_b[i])
          return cmp unless cmp == 0
        end
        (arr_a.size <=> arr_b.size) || 0
      else
        (a.as(JSON::Any).to_json <=> b.as(JSON::Any).to_json) || 0
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
      groups = Hash(String, Array(EmittedRow)).new { |hash, key| hash[key] = [] of EmittedRow }
      rows.each { |emitted_row| groups[group_key_for(emitted_row.key, group_level).to_json] << emitted_row }
      groups.map do |_, grp|
        gkey = group_key_for(grp.first.key, group_level)
        JSON::Any.new({
          "key"   => gkey || JSON::Any.new(nil),
          "value" => reduce_values(fn, grp.map(&.value)),
        } of String => JSON::Any)
      end
    end

    private def build_query_rows(
      paged : Array(EmittedRow),
      include_docs : Bool,
      doc_lookup : Hash(String, JSON::Any),
    ) : Array(JSON::Any)
      paged.map do |row|
        entry = {
          "id"    => JSON::Any.new(row.doc_id),
          "key"   => row.key || JSON::Any.new(nil),
          "value" => row.value || JSON::Any.new(nil),
        } of String => JSON::Any
        entry["doc"] = doc_lookup[row.doc_id] if include_docs
        JSON::Any.new(entry)
      end
    end

    # Navigates a dot-notation path into a JSON object (e.g. "address.city").
    # Returns nil if any segment is missing or an intermediate node is not an object.
    private def doc_field(doc : JSON::Any, path : String) : JSON::Any?
      path.split('.').reduce(doc.as(JSON::Any?)) do |cur, seg|
        cur.try(&.as_h?).try(&.[seg]?)
      end
    end

    # Returns the Mango type name for a JSON value.
    private def json_type_name(v : JSON::Any) : String
      case v.raw
      when Nil            then "null"
      when Bool           then "boolean"
      when Int64, Float64 then "number"
      when String         then "string"
      when Array          then "array"
      when Hash           then "object"
      else                     "null"
      end
    end

    # Top-level selector: each key is either a logical combinator ($and/$or/$nor/$not)
    # or a field path. All conditions are implicitly ANDed.
    # An empty selector {} matches everything.
    private def match_selector?(doc : JSON::Any, selector : JSON::Any) : Bool
      selector.as_h.all? do |key, condition|
        case key
        when "$and" then condition.as_a.all? { |sub| match_selector?(doc, sub) }
        when "$or"  then condition.as_a.any? { |sub| match_selector?(doc, sub) }
        when "$nor" then condition.as_a.none? { |sub| match_selector?(doc, sub) }
        when "$not" then !match_selector?(doc, condition)
        else             match_condition?(doc_field(doc, key), condition)
        end
      end
    end

    # Evaluates a single field's condition. Bare value = implicit $eq.
    # A Hash with $-prefixed keys = operator map (all operators must pass).
    private def match_condition?(field_val : JSON::Any?, condition : JSON::Any) : Bool
      if (h = condition.as_h?) && h.any? { |k, _| k.starts_with?('$') }
        h.all? { |op, operand| match_operator?(field_val, op, operand) }
      else
        compare_json_keys(field_val, condition) == 0
      end
    end

    # Dispatches a single Mango operator against a field value.
    private def match_operator?(field_val : JSON::Any?, op : String, operand : JSON::Any) : Bool
      case op
      when "$eq", "$ne", "$lt", "$lte", "$gt", "$gte"
        match_relational_op?(field_val, op, operand)
      when "$exists", "$type", "$in", "$nin", "$not"
        match_membership_op?(field_val, op, operand)
      when "$all", "$size", "$mod", "$regex", "$elemMatch"
        match_structure_op?(field_val, op, operand)
      else
        raise ArgumentError.new("Unknown operator: #{op}")
      end
    end

    private def match_relational_op?(field_val : JSON::Any?, op : String, operand : JSON::Any) : Bool
      case op
      when "$eq"  then compare_json_keys(field_val, operand) == 0
      when "$ne"  then compare_json_keys(field_val, operand) != 0
      when "$lt"  then !!field_val && compare_json_keys(field_val, operand) < 0
      when "$lte" then !!field_val && compare_json_keys(field_val, operand) <= 0
      when "$gt"  then !!field_val && compare_json_keys(field_val, operand) > 0
      else             !!field_val && compare_json_keys(field_val, operand) >= 0 # $gte
      end
    end

    private def match_membership_op?(field_val : JSON::Any?, op : String, operand : JSON::Any) : Bool
      case op
      when "$exists" then operand.as_bool ? !field_val.nil? : field_val.nil?
      when "$type"
        return false unless field_val
        json_type_name(field_val.as(JSON::Any)) == operand.as_s
      when "$in"  then operand.as_a.any? { |elem| compare_json_keys(field_val, elem) == 0 }
      when "$nin" then operand.as_a.none? { |elem| compare_json_keys(field_val, elem) == 0 }
      else             !match_condition?(field_val, operand) # $not
      end
    end

    private def match_structure_op?(field_val : JSON::Any?, op : String, operand : JSON::Any) : Bool
      return match_mod_op?(field_val, operand) if op == "$mod"
      return match_regex_op?(field_val, operand) if op == "$regex"
      return false unless field_val && field_val.raw.is_a?(Array)
      arr = field_val.as_a
      case op
      when "$all"  then match_all_op?(arr, operand.as_a)
      when "$size" then arr.size == operand.raw.as(Int64 | Float64).to_i
      else              arr.any? { |elem| match_selector?(elem, operand) } # $elemMatch
      end
    end

    private def match_all_op?(arr : Array(JSON::Any), required : Array(JSON::Any)) : Bool
      required.all? { |elem| arr.any? { |item| compare_json_keys(item, elem) == 0 } }
    end

    private def match_mod_op?(field_val : JSON::Any?, operand : JSON::Any) : Bool
      return false unless field_val
      num = field_val.raw
      return false unless num.is_a?(Int64) || num.is_a?(Float64)
      divisor = operand.as_a[0].raw.as(Int64 | Float64).to_i64
      remainder = operand.as_a[1].raw.as(Int64 | Float64).to_i64
      num.as(Int64 | Float64).to_i64 % divisor == remainder
    end

    private def match_regex_op?(field_val : JSON::Any?, operand : JSON::Any) : Bool
      return false unless field_val && field_val.raw.is_a?(String)
      Regex.new(operand.as_s).matches?(field_val.as_s)
    end

    # Sorts docs by the sort spec. Each element is a bare string (asc)
    # or a single-key hash with "asc"/"desc" value.
    private def apply_find_sort(docs : Array(JSON::Any), sort : Array(JSON::Any)) : Array(JSON::Any)
      sort_spec = sort.map do |item|
        case item.raw
        when String then {item.as_s, false}
        when Hash
          field, dir = item.as_h.first
          {field, dir.as_s == "desc"}
        else raise ArgumentError.new("Invalid sort item: #{item.inspect}")
        end
      end
      docs.sort do |lhs, rhs|
        result = 0
        sort_spec.each do |(field, desc)|
          cmp = compare_json_keys(doc_field(lhs, field), doc_field(rhs, field))
          cmp = -cmp if desc
          result = cmp
          break unless result == 0
        end
        result
      end
    end

    # Projects a doc to only the listed field names (dot-notation stored flat).
    private def project_doc_fields(doc : JSON::Any, fields : Array(String)) : JSON::Any
      h = {} of String => JSON::Any
      fields.each do |field_name|
        val = doc_field(doc, field_name)
        h[field_name] = val if val
      end
      JSON::Any.new(h)
    end
  end
end
