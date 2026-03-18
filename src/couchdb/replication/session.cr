module CouchDB
  module Replication
    # Result object returned by `Database#replicate_to`, `#replicate_from`, and `#sync`.
    #
    # Carries transfer statistics for one replication run. Check `ok` first; if it is
    # `false`, `error` contains the failure message.
    #
    # ```crystal
    # session = local.replicate_to(remote)
    # puts session.ok            # true / false
    # puts session.docs_written  # number of documents transferred
    # puts session.last_seq      # last sequence number processed
    # ```
    class Session
      # Name or URL of the replication source.
      getter source_url : String
      # Name or URL of the replication target.
      getter target_url : String
      # Total number of documents fetched from the source.
      getter docs_read : Int32
      # Total number of documents successfully written to the target.
      getter docs_written : Int32
      # Total number of documents that failed to write to the target.
      getter doc_write_failures : Int32
      # Last sequence number processed by this replication run.
      getter last_seq : String
      # `true` when replication completed without error, `false` otherwise.
      getter ok : Bool
      # Error message when `ok` is `false`; `nil` on success.
      getter error : String?

      # Creates a new session for a replication from *source_url* to *target_url*.
      # All counters start at zero and `ok` starts as `false` until `finish!` is called.
      def initialize(@source_url : String, @target_url : String)
        @docs_read = 0
        @docs_written = 0
        @doc_write_failures = 0
        @last_seq = "0"
        @ok = false
        @error = nil
      end

      # Accumulates statistics from one replication batch. Called by `Replicator` after
      # each batch of documents is processed.
      def record_batch(read : Int32, written : Int32, failures : Int32, seq : String)
        @docs_read += read
        @docs_written += written
        @doc_write_failures += failures
        @last_seq = seq
      end

      # Marks the session as successful. Called by `Replicator` after all batches complete.
      def finish!
        @ok = true
      end

      # Records an error message and marks the session as failed.
      def fail!(message : String)
        @error = message
        @ok = false
      end

      # Returns a human-readable summary of the replication session.
      def to_s(io : IO)
        io << "Replication(#{source_url} → #{target_url}, ok=#{ok}, "
        io << "docs_read=#{docs_read}, docs_written=#{docs_written}, "
        io << "last_seq=#{last_seq})"
      end
    end
  end
end
