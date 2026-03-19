require "base64"
require "db"
require "sqlite3"
require "./base"

module CouchDB
  module Adapter
    # Local-storage adapter backed by SQLite3.
    #
    # All reads and writes go to a single SQLite file, making the database
    # available without any network connection. Use `":memory:"` for a transient
    # in-memory database (useful for testing).
    #
    # Four tables underpin the adapter:
    # - `docs` — every revision of every document (enables `revs_diff`)
    # - `revs` — parent-revision linkage tree
    # - `local_docs` — `_local/` documents (checkpoints, never replicated)
    # - `update_seq` — append-only sequence log; ROWID is the `update_seq`
    #
    # The winning revision for a given document is the one with the highest
    # `seq` value. Deleted documents are soft-deleted (a `deleted=1` row is
    # stored) so their revision history remains available for replication.
    class SQLite
      include Adapter

      SCHEMA = <<-SQL
        CREATE TABLE IF NOT EXISTS docs (
          id      TEXT    NOT NULL,
          rev     TEXT    NOT NULL,
          seq     INTEGER NOT NULL,
          deleted INTEGER NOT NULL DEFAULT 0,
          body    TEXT    NOT NULL,
          PRIMARY KEY (id, rev)
        );
        CREATE INDEX IF NOT EXISTS idx_docs_id_seq ON docs(id, seq DESC);

        CREATE TABLE IF NOT EXISTS revs (
          id         TEXT NOT NULL,
          rev        TEXT NOT NULL,
          parent_rev TEXT,
          PRIMARY KEY (id, rev)
        );

        CREATE TABLE IF NOT EXISTS local_docs (
          id   TEXT PRIMARY KEY,
          body TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS update_seq (
          seq     INTEGER PRIMARY KEY AUTOINCREMENT,
          doc_id  TEXT NOT NULL,
          doc_rev TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS attachments (
          doc_id       TEXT NOT NULL,
          name         TEXT NOT NULL,
          content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
          data         BLOB NOT NULL,
          PRIMARY KEY (doc_id, name)
        );
      SQL

      @db : DB::Database
      @db_name : String

      # Opens (or creates) a SQLite3 database at *path*.
      #
      # Pass `":memory:"` for a transient in-memory database. If *path* does not end
      # in `.db`, the extension is appended automatically by `Database.new`. The
      # connection pool is limited to a single connection (`max_pool_size=1`) to
      # keep `:memory:` databases on one connection and avoid SQLite write-lock
      # contention on file databases.
      def initialize(path : String)
        # max_pool_size=1 ensures :memory: databases stay on a single connection
        # and also avoids SQLite write-lock contention
        @db = DB.open("sqlite3:#{path}?max_pool_size=1")
        # Use path as db_name for stable checkpoint IDs; random suffix for :memory:
        @db_name = path == ":memory:" ? "sqlite_#{Random::Secure.hex(8)}" : path
        migrate!
      end

      # Closes the underlying database connection pool.
      def close
        @db.close
      end

      # SQLite implementation of `Adapter#info`. See `Adapter#info` for the contract.
      def info : NamedTuple(db_name: String, doc_count: Int64, update_seq: Int64)
        doc_count = @db.scalar(<<-SQL).as(Int64)
          SELECT COUNT(*) FROM (
            SELECT d.id FROM docs d
            INNER JOIN (
              SELECT id, MAX(seq) AS max_seq FROM docs GROUP BY id
            ) w ON d.id = w.id AND d.seq = w.max_seq
            WHERE d.deleted = 0
          )
        SQL
        last_seq = @db.scalar("SELECT COALESCE(MAX(seq), 0) FROM update_seq").as(Int64)
        {db_name: @db_name, doc_count: doc_count, update_seq: last_seq}
      end

      # SQLite implementation of `Adapter#get`. See `Adapter#get` for the contract.
      def get(id : String) : Document
        row = winning_rev_row(id)
        raise NotFound.new(id) unless row
        _id_val, _rev, _seq, deleted, body = row
        raise NotFound.new(id) if deleted == 1
        parse_doc(body)
      end

      # SQLite implementation of `Adapter#put`. See `Adapter#put` for the contract.
      def put(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
        id = doc.id
        raise BadRequest.new("Missing _id") if id.empty?

        existing_rev = begin
          winning_rev_for(id)
        rescue NotFound
          nil
        end

        provided_rev = doc.rev

        if existing_rev
          raise Conflict.new(id, provided_rev || "") if provided_rev != existing_rev
        else
          raise Conflict.new(id, provided_rev || "") if provided_rev && !provided_rev.empty?
        end

        new_rev = doc.next_rev
        store_doc(id, new_rev, doc, deleted: false, parent_rev: existing_rev)
        {ok: true, id: id, rev: new_rev}
      end

      # SQLite implementation of `Adapter#remove`. See `Adapter#remove` for the contract.
      def remove(id : String, rev : String) : NamedTuple(ok: Bool)
        existing = get(id)
        existing_rev = existing.rev || ""
        raise Conflict.new(id, rev) if rev != existing_rev

        existing.deleted = true
        new_rev = existing.next_rev
        store_doc(id, new_rev, existing, deleted: true, parent_rev: existing_rev)
        {ok: true}
      end

      # SQLite implementation of `Adapter#bulk_docs`. See `Adapter#bulk_docs` for the contract.
      def bulk_docs(docs : Array(Document), new_edits : Bool = true) : Array(NamedTuple(id: String, rev: String, ok: Bool))
        results = [] of NamedTuple(id: String, rev: String, ok: Bool)

        # Use explicit BEGIN/COMMIT so inner queries can reuse the single connection
        # (crystal-db's @db.transaction {} holds the connection for the whole block)
        @db.exec("BEGIN")
        begin
          docs.each do |doc|
            id = doc.id
            raise BadRequest.new("Missing _id") if id.empty?
            if new_edits
              results << bulk_docs_new_edit(doc, id)
            else
              results << bulk_docs_replication(doc, id)
            end
          end
          @db.exec("COMMIT")
        rescue ex
          @db.exec("ROLLBACK")
          raise ex
        end

        results
      end

      # SQLite implementation of `Adapter#all_docs`. See `Adapter#all_docs` for the contract.
      def all_docs(include_docs : Bool = false, limit : Int32? = nil, skip : Int32 = 0,
                   startkey : String? = nil, endkey : String? = nil) : NamedTuple(
        total_rows: Int64,
        offset: Int32,
        rows: Array(JSON::Any))
        # total_rows is always the unfiltered count — matches CouchDB behaviour
        total = @db.scalar(
          "SELECT COUNT(DISTINCT id) FROM docs WHERE deleted = 0"
        ).as(Int64)

        limit_val = limit || -1
        rows = [] of JSON::Any

        sql, key_params = all_docs_sql(startkey, endkey)
        @db.query(sql, args: key_params + [limit_val, skip] of DB::Any) do |result_set|
          build_all_docs_rows(result_set, rows, include_docs)
        end

        {total_rows: total, offset: skip, rows: rows}
      end

      # SQLite implementation of `Adapter#changes`. See `Adapter#changes` for the contract.
      def changes(since : String = "0", limit : Int32? = nil, include_docs : Bool = false) : NamedTuple(
        last_seq: String,
        results: Array(JSON::Any))
        since_seq = since.to_i64? || 0_i64
        limit_val = limit || -1

        sql = <<-SQL
          SELECT u.seq, u.doc_id, u.doc_rev, d.deleted, d.body
          FROM update_seq u
          JOIN docs d ON d.id = u.doc_id AND d.rev = u.doc_rev
          WHERE u.seq > ?
          ORDER BY u.seq ASC
          LIMIT ?
        SQL

        results = [] of JSON::Any
        last_seq = since_seq

        @db.query(sql, since_seq, limit_val) do |result_set|
          result_set.each do
            seq = result_set.read(Int64)
            doc_id = result_set.read(String)
            doc_rev = result_set.read(String)
            deleted = result_set.read(Int64) == 1
            body = result_set.read(String)

            last_seq = seq
            results << build_change_json(seq, doc_id, doc_rev, deleted, body, include_docs)
          end
        end

        {last_seq: last_seq.to_s, results: results}
      end

      # SQLite implementation of `Adapter#changes_feed`. See `Adapter#changes_feed` for the contract.
      def changes_feed(since : String = "0", heartbeat : Int32 = 1000,
                       include_docs : Bool = false, & : JSON::Any -> _)
        current_seq = since.to_i64? || 0_i64

        loop do
          sql = <<-SQL
            SELECT u.seq, u.doc_id, u.doc_rev, d.deleted, d.body
            FROM update_seq u
            JOIN docs d ON d.id = u.doc_id AND d.rev = u.doc_rev
            WHERE u.seq > ?
            ORDER BY u.seq ASC
          SQL

          @db.query(sql, current_seq) do |result_set|
            result_set.each do
              seq = result_set.read(Int64)
              doc_id = result_set.read(String)
              doc_rev = result_set.read(String)
              deleted = result_set.read(Int64) == 1
              body = result_set.read(String)

              current_seq = seq
              yield build_change_json(seq, doc_id, doc_rev, deleted, body, include_docs)
            end
          end

          sleep heartbeat.milliseconds
        end
      end

      # SQLite implementation of `Adapter#revs_diff`. See `Adapter#revs_diff` for the contract.
      def revs_diff(id_revs : Hash(String, Array(String))) : Hash(String, NamedTuple(missing: Array(String)))
        result = {} of String => NamedTuple(missing: Array(String))

        id_revs.each do |id, revs|
          missing = revs.reject do |rev|
            @db.scalar(
              "SELECT COUNT(*) FROM docs WHERE id = ? AND rev = ?", id, rev
            ).as(Int64) > 0
          end

          result[id] = {missing: missing} unless missing.empty?
        end

        result
      end

      # SQLite implementation of `Adapter#bulk_get`. See `Adapter#bulk_get` for the contract.
      def bulk_get(id_revs : Array(NamedTuple(id: String, rev: String))) : Array(Document)
        docs = [] of Document

        id_revs.each do |pair|
          row = @db.query_one?(
            "SELECT body FROM docs WHERE id = ? AND rev = ?",
            pair[:id], pair[:rev],
            as: String
          )
          docs << parse_doc(row) if row
        end

        docs
      end

      # SQLite implementation of `Adapter#get_local`. See `Adapter#get_local` for the contract.
      def get_local(id : String) : Document
        full_id = local_id(id)
        row = @db.query_one?(
          "SELECT body FROM local_docs WHERE id = ?", full_id, as: String
        )
        raise NotFound.new(full_id) unless row
        parse_doc(row)
      end

      # SQLite implementation of `Adapter#put_local`. See `Adapter#put_local` for the contract.
      def put_local(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
        raw_id = doc.id
        raise BadRequest.new("Missing _id") if raw_id.empty?
        full_id = local_id(raw_id)

        new_rev = doc.next_rev

        # Clone to avoid mutating the caller's document
        stored = Document.from_json(doc.to_json)
        stored.id = full_id
        stored.rev = new_rev

        @db.exec(
          "INSERT OR REPLACE INTO local_docs (id, body) VALUES (?, ?)",
          full_id, stored.to_json
        )

        {ok: true, id: full_id, rev: new_rev}
      end

      # SQLite implementation of `Adapter#get_attachment`. See `Adapter#get_attachment` for the contract.
      def get_attachment(id : String, attname : String) : NamedTuple(data: Bytes, content_type: String)
        get(id) # raises NotFound if document missing or deleted
        row = @db.query_one?(
          "SELECT content_type, data FROM attachments WHERE doc_id = ? AND name = ?",
          id, attname, as: {String, Bytes}
        )
        raise NotFound.new("#{id}/#{attname}") unless row
        ct, data = row
        {data: data, content_type: ct}
      end

      # SQLite implementation of `Adapter#put_attachment`. See `Adapter#put_attachment` for the contract.
      def put_attachment(id : String, attname : String, rev : String,
                         data : Bytes, content_type : String) : NamedTuple(ok: Bool, id: String, rev: String)
        existing = get(id) # raises NotFound if missing/deleted
        raise Conflict.new(id, rev) if existing.rev != rev

        attaches = existing["_attachments"]?.try(&.as_h?) || {} of String => JSON::Any
        attaches[attname] = JSON::Any.new({
          "content_type" => JSON::Any.new(content_type),
          "length"       => JSON::Any.new(data.size.to_i64),
          "stub"         => JSON::Any.new(true),
        } of String => JSON::Any)
        existing["_attachments"] = JSON::Any.new(attaches)

        new_rev = existing.next_rev
        store_doc(id, new_rev, existing, deleted: false, parent_rev: rev)
        @db.exec(
          "INSERT OR REPLACE INTO attachments (doc_id, name, content_type, data) VALUES (?, ?, ?, ?)",
          id, attname, content_type, data
        )
        {ok: true, id: id, rev: new_rev}
      end

      # SQLite implementation of `Adapter#delete_attachment`. See `Adapter#delete_attachment` for the contract.
      def delete_attachment(id : String, attname : String, rev : String) : NamedTuple(ok: Bool, id: String, rev: String)
        existing = get(id) # raises NotFound if missing/deleted
        raise Conflict.new(id, rev) if existing.rev != rev

        attaches = existing["_attachments"]?.try(&.as_h?) || {} of String => JSON::Any
        attaches.delete(attname)
        existing["_attachments"] = JSON::Any.new(attaches)

        new_rev = existing.next_rev
        store_doc(id, new_rev, existing, deleted: false, parent_rev: rev)
        @db.exec("DELETE FROM attachments WHERE doc_id = ? AND name = ?", id, attname)
        {ok: true, id: id, rev: new_rev}
      end

      private def all_docs_sql(startkey : String?, endkey : String?) : {String, Array(DB::Any)}
        conditions = [] of String
        params = [] of DB::Any
        if startkey
          conditions << "d.id >= ?"
          params << startkey
        end
        if endkey
          conditions << "d.id <= ?"
          params << endkey
        end
        where = conditions.empty? ? "" : "WHERE #{conditions.join(" AND ")}"
        sql = <<-SQL
          SELECT d.id, d.rev, d.body
          FROM docs d
          INNER JOIN (
            SELECT id, MAX(seq) AS max_seq FROM docs WHERE deleted = 0 GROUP BY id
          ) w ON d.id = w.id AND d.seq = w.max_seq
          #{where}
          ORDER BY d.id
          LIMIT ? OFFSET ?
        SQL
        {sql, params}
      end

      private def build_change_json(seq : Int64, doc_id : String, doc_rev : String,
                                    deleted : Bool, body : String,
                                    include_docs : Bool) : JSON::Any
        entry = {
          "seq"     => JSON::Any.new(seq.to_s),
          "id"      => JSON::Any.new(doc_id),
          "changes" => JSON::Any.new([JSON::Any.new({"rev" => JSON::Any.new(doc_rev)} of String => JSON::Any)] of JSON::Any),
        } of String => JSON::Any
        entry["deleted"] = JSON::Any.new(true) if deleted
        entry["doc"] = JSON.parse(body) if include_docs
        JSON::Any.new(entry)
      end

      private def build_all_docs_rows(result_set : DB::ResultSet, rows : Array(JSON::Any), include_docs : Bool)
        result_set.each do
          doc_id = result_set.read(String)
          rev = result_set.read(String)
          body = result_set.read(String)

          row_data = {
            "id"    => JSON::Any.new(doc_id),
            "key"   => JSON::Any.new(doc_id),
            "value" => JSON::Any.new({"rev" => JSON::Any.new(rev)} of String => JSON::Any),
          } of String => JSON::Any

          row_data["doc"] = JSON.parse(body) if include_docs

          rows << JSON::Any.new(row_data)
        end
      end

      private def bulk_docs_new_edit(doc : Document, id : String) : NamedTuple(id: String, rev: String, ok: Bool)
        deleted = doc.deleted?
        existing_rev = begin
          winning_rev_for(id)
        rescue NotFound
          nil
        end
        provided_rev = doc.rev
        if existing_rev
          return {id: id, rev: provided_rev || "", ok: false} if provided_rev != existing_rev
        else
          return {id: id, rev: provided_rev, ok: false} if provided_rev && !provided_rev.empty?
        end
        new_rev = doc.next_rev
        store_doc(id, new_rev, doc, deleted: deleted, parent_rev: existing_rev)
        {id: id, rev: new_rev, ok: true}
      end

      private def bulk_docs_replication(doc : Document, id : String) : NamedTuple(id: String, rev: String, ok: Bool)
        deleted = doc.deleted?
        rev = doc.rev || raise BadRequest.new("Missing _rev for new_edits=false")
        parent_rev = doc.json_unmapped["_revisions"]?.try(&.as_h?).try { |rev_map|
          ids = rev_map["ids"]?.try(&.as_a?)
          start = rev_map["start"]?.try(&.as_i?)
          if ids && start && ids.size > 1
            "#{start - 1}-#{ids[1].as_s}"
          end
        }
        store_doc(id, rev, doc, deleted: deleted, parent_rev: parent_rev)
        {id: id, rev: rev, ok: true}
      end

      private def migrate!
        SCHEMA.split(";").each do |stmt|
          s = stmt.strip
          @db.exec(s) unless s.empty?
        end
      end

      private def winning_rev_row(id : String)
        @db.query_one?(
          <<-SQL,
            SELECT d.id, d.rev, d.seq, d.deleted, d.body
            FROM docs d
            INNER JOIN (
              SELECT id, MAX(seq) AS max_seq FROM docs WHERE id = ? GROUP BY id
            ) w ON d.id = w.id AND d.seq = w.max_seq
          SQL
          id, as: {String, String, Int64, Int64, String}
        )
      end

      private def winning_rev_for(id : String) : String
        row = @db.query_one?(
          <<-SQL,
            SELECT d.rev
            FROM docs d
            INNER JOIN (
              SELECT id, MAX(seq) AS max_seq FROM docs WHERE id = ? GROUP BY id
            ) w ON d.id = w.id AND d.seq = w.max_seq
          SQL
          id, as: String
        )
        raise NotFound.new(id) unless row
        row
      end

      private def extract_inline_attachments(id : String, doc : Document)
        raw = doc["_attachments"]?.try(&.as_h?) || return
        updated = false

        raw.each do |attname, meta|
          meta_h = meta.as_h? || next
          inline_data = meta_h["data"]?.try(&.as_s?) || next

          bytes = Base64.decode(inline_data)
          ct = meta_h["content_type"]?.try(&.as_s?) || "application/octet-stream"

          @db.exec(
            "INSERT OR REPLACE INTO attachments (doc_id, name, content_type, data) VALUES (?, ?, ?, ?)",
            id, attname, ct, bytes
          )

          raw[attname] = JSON::Any.new({
            "content_type" => JSON::Any.new(ct),
            "length"       => JSON::Any.new(bytes.size.to_i64),
            "stub"         => JSON::Any.new(true),
          } of String => JSON::Any)
          updated = true
        end

        doc["_attachments"] = JSON::Any.new(raw) if updated
      end

      private def store_doc(id : String, rev : String, doc : Document, deleted : Bool, parent_rev : String?)
        extract_inline_attachments(id, doc)
        # Clone to set the canonical id/rev without mutating the caller's document
        stored = Document.from_json(doc.to_json)
        stored.id = id
        stored.rev = rev
        body = stored.to_json

        # Insert into update_seq first to get the AUTOINCREMENT seq
        @db.exec(
          "INSERT INTO update_seq (doc_id, doc_rev) VALUES (?, ?)",
          id, rev
        )
        seq = @db.scalar("SELECT last_insert_rowid()").as(Int64)

        @db.exec(
          "INSERT OR REPLACE INTO docs (id, rev, seq, deleted, body) VALUES (?, ?, ?, ?, ?)",
          id, rev, seq, deleted ? 1 : 0, body
        )

        @db.exec(
          "INSERT OR IGNORE INTO revs (id, rev, parent_rev) VALUES (?, ?, ?)",
          id, rev, parent_rev
        )
      end

      private def local_id(id : String) : String
        id.starts_with?("_local/") ? id : "_local/#{id}"
      end

      private def parse_doc(body : String) : Document
        Document.from_json(body)
      end
    end
  end
end
