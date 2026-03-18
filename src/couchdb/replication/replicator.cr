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
      def initialize(@source : Adapter, @target : Adapter)
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
        checkpoint = Checkpoint.new(@source, @target)
        since = checkpoint.read

        loop do
          # Step 4: Get changes feed
          changes = @source.changes(since: since, limit: BATCH_SIZE, include_docs: false)
          results = changes[:results]
          last_seq = changes[:last_seq]

          break if results.empty?

          # Build id → revs map from changes
          id_revs = {} of String => Array(String)
          results.each do |change|
            id = change["id"]?.try(&.as_s?) || next
            revs = change["changes"]?.try(&.as_a?).try(&.map { |entry|
              entry["rev"]?.try(&.as_s?) || ""
            }.reject(&.empty?)) || [] of String
            id_revs[id] = revs
          end

          # Step 5: revs_diff — only missing revisions
          missing = @target.revs_diff(id_revs)

          docs_read = 0
          docs_written = 0
          failures = 0

          unless missing.empty?
            # Step 6: Fetch from source and upload to target
            pairs = missing.flat_map do |id, info|
              info[:missing].map { |rev| {id: id, rev: rev} }
            end

            fetched = @source.bulk_get(pairs)
            docs_read = fetched.size

            unless fetched.empty?
              write_results = @target.bulk_docs(fetched, new_edits: false)
              write_results.each do |write_result|
                if write_result[:ok]
                  docs_written += 1
                else
                  failures += 1
                end
              end
            end
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
    end
  end
end
