require "digest/md5"

module CouchDB
  module Replication
    # Manages resumable replication checkpoints stored as `_local/` documents.
    #
    # A checkpoint records the last sequence number successfully replicated. When a
    # replication run is interrupted, the next run reads the checkpoint and resumes
    # from where the previous run left off rather than reprocessing the entire
    # changes feed.
    #
    # Checkpoints are stored on both the source and the target adapter. The starting
    # sequence is the minimum of the two checkpoints so that neither side can get
    # ahead of the other.
    class Checkpoint
      CHECKPOINT_PREFIX = "_local/"

      @checkpoint_id : String

      # Creates a checkpoint manager for the given *source* and *target* adapters.
      # The checkpoint document ID is derived from the MD5 of both database names,
      # giving a stable, collision-resistant identifier for this replication pair.
      def initialize(@source : Adapter, @target : Adapter)
        @checkpoint_id = compute_id
      end

      # Returns the sequence number to start from for this replication.
      #
      # Reads the checkpoint from both source and target and returns the minimum,
      # ensuring that neither side can skip changes the other has not yet seen.
      # Returns `"0"` when no checkpoint exists.
      def read : String
        source_seq = read_from(@source)
        target_seq = read_from(@target)

        # Use min of both so we never skip changes
        source_n = source_seq.to_i64? || 0_i64
        target_n = target_seq.to_i64? || 0_i64
        {source_n, target_n}.min.to_s
      end

      # Writes *seq* as the new checkpoint on both source and target.
      # Called by `Replicator` after each successfully processed batch.
      def write(seq : String)
        doc = Document.new
        doc.id = checkpoint_doc_id
        doc["last_seq"] = JSON::Any.new(seq)
        doc["session_id"] = JSON::Any.new(Random::Secure.hex(16))

        write_to(@source, doc)
        write_to(@target, doc)
      end

      private def read_from(adapter : Adapter) : String
        doc = adapter.get_local(checkpoint_doc_id)
        doc["last_seq"]?.try(&.as_s?) || "0"
      rescue NotFound
        "0"
      end

      private def write_to(adapter : Adapter, doc : Document)
        # Clone per adapter to avoid cross-contaminating the rev field
        updated = Document.from_json(doc.to_json)
        existing = adapter.get_local(checkpoint_doc_id)
        updated.rev = existing.rev
        adapter.put_local(updated)
      rescue NotFound
        adapter.put_local(doc)
      end

      private def checkpoint_doc_id : String
        "#{CHECKPOINT_PREFIX}#{@checkpoint_id}"
      end

      private def compute_id : String
        source_info = @source.info
        target_info = @target.info
        Digest::MD5.hexdigest("#{source_info[:db_name]}:#{target_info[:db_name]}")
      end
    end
  end
end
