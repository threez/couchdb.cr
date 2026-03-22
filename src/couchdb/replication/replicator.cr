require "./session"
require "./checkpoint"

module CouchDB
  module Replication
    BATCH_SIZE = 100

    # Implements the 7-step CouchDB replication protocol between two adapters.
    #
    # Replication is resumable: a `Checkpoint` document is written to both source
    # and target after every batch of `BATCH_SIZE` (100) documents so that an
    # interrupted run can continue from where it left off.
    #
    # Steps per batch:
    # 1. Verify peers (fetch `info` from source and target)
    # 2. Read checkpoint (determine `since` sequence)
    # 3. Fetch changes feed from source
    # 4. Call `revs_diff` on target to find missing revisions
    # 5. Fetch missing documents from source via `bulk_get`
    # 6. Write documents to target via `bulk_docs(new_edits: false)`
    # 7. Write checkpoint to both source and target
    class Replicator
      # Creates a replicator that will copy documents from *source* to *target*.
      #
      # *doc_ids* limits replication to a specific set of document IDs (applied
      # before `revs_diff` to avoid unnecessary network calls).
      #
      # *filter* is an arbitrary predicate applied after `bulk_get`; only documents
      # for which it returns `true` are written to the target.
      #
      # Both options are composable: set both to filter by ID *and* by content.
      def initialize(
        @source : Adapter,
        @target : Adapter,
        @doc_ids : Array(String)? = nil,
        @filter : Proc(Document, Bool)? = nil,
        @checkpoint_store : Adapter? = nil,
      )
      end

      # Runs the full replication loop and returns a `Session` with transfer statistics.
      #
      # The loop continues until the changes feed returns fewer results than
      # `BATCH_SIZE`, indicating that all pending changes have been transferred.
      # On error, returns a failed `Session` with the exception message in `error`.
      def replicate : Session
        source_info = @source.info
        target_info = @target.info
        session = Session.new(source_info[:db_name], target_info[:db_name])

        # Step 1 & 2: Verify peers (info already fetched above — raises on failure)

        # Step 3: Read checkpoint
        checkpoint = Checkpoint.new(@source, @target, @checkpoint_store)
        since = checkpoint.read

        loop do
          # Step 4: Get changes feed
          changes = @source.changes(since: since, limit: BATCH_SIZE, include_docs: false)
          results = changes[:results]
          last_seq = changes[:last_seq]

          break if results.empty?

          id_revs = build_id_revs(results)

          # Step 5: revs_diff — only missing revisions
          missing = @target.revs_diff(id_revs)

          docs_read = 0
          docs_written = 0
          failures = 0

          unless missing.empty?
            pairs = missing.flat_map { |id, info| info[:missing].map { |rev| {id: id, rev: rev} } }
            fetched = @source.bulk_get(pairs)
            if proc = @filter
              fetched = fetched.select { |doc| proc.call(doc) }
            end
            docs_read = fetched.size
            docs_written, failures = write_docs(fetched) unless fetched.empty?
          end

          session.record_batch(docs_read, docs_written, failures, last_seq)

          # Step 7: Write checkpoint after each batch
          checkpoint.write(last_seq)

          since = last_seq

          # Stop when we got fewer results than the batch size
          break if results.size < BATCH_SIZE
        end

        session.finish!
        session
      rescue ex : Exception
        session = Session.new(@source.info[:db_name], @target.info[:db_name]) rescue Session.new("source", "target")
        session.fail!(ex.message || "unknown error")
        session
      end

      private def build_id_revs(results : Array(JSON::Any)) : Hash(String, Array(String))
        id_revs = {} of String => Array(String)
        results.each do |change|
          id = change["id"]?.try(&.as_s?) || next
          next if (ids = @doc_ids) && !ids.includes?(id)
          revs = change["changes"]?.try(&.as_a?).try(&.map { |entry|
            entry["rev"]?.try(&.as_s?) || ""
          }.reject(&.empty?)) || [] of String
          id_revs[id] = revs
        end
        id_revs
      end

      private def write_docs(docs : Array(Document)) : {Int32, Int32}
        written = 0
        failures = 0
        @target.bulk_docs(docs, new_edits: false).each do |result|
          if result[:ok]
            written += 1
          else
            failures += 1
          end
        end
        {written, failures}
      end
    end
  end
end
