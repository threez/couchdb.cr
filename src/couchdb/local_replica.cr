require "./database"

module CouchDB
  # A `Database` backed by a local SQLite file that continuously syncs with a
  # remote CouchDB in both directions. All reads operate on the local store for
  # low latency and offline capability.
  #
  # Checkpoints are stored on the remote adapter only (`_local/` documents),
  # so the local file can be deleted and recreated without losing sync progress.
  #
  # Two write modes are available:
  #
  # - **local** (default): `put`/`remove` write to local SQLite; background
  #   fibers push changes to the remote and pull remote changes down.
  # - **upstream**: `put`/`remove` write to the remote and block until the
  #   change has been replicated to local (via the pull fiber). Reads always
  #   come from local.
  #
  # Use `Database.local_replica` to create instances.
  #
  # ```
  # db = CouchDB::Database.local_replica("notes.db",
  #   "https://user:pass@mycouch.example.com/notes")
  # db.on_sync_error { |dir, ex| Log.error { "#{dir}: #{ex.message}" } }
  # db.put(note)     # writes locally; background push syncs to remote
  # db.get("note-1") # reads from local
  # db.close         # stops sync fibers, closes both adapters
  # ```
  class LocalReplica < Database
    # Callback invoked when a background replication run fails.
    # The first argument is `"push"` or `"pull"`; the second is the exception.
    alias SyncErrorCallback = Proc(String, Exception, Nil)

    UPSTREAM_WAIT_INTERVAL = 10.milliseconds
    UPSTREAM_WAIT_TIMEOUT  = 5000.milliseconds

    @remote : Database
    @running : Bool
    @write_upstream : Bool
    @on_sync_error : SyncErrorCallback?

    # Registers a block called when a background sync run fails.
    def on_sync_error(&block : String, Exception -> Nil)
      @on_sync_error = block
    end

    # :nodoc: Use `Database.local_replica` instead.
    def initialize(
      local_path : String,
      remote_url : String,
      heartbeat : Int32 = 2000,
      write_upstream : Bool = false,
    )
      super(local_path)
      @remote = Database.new(remote_url)
      @running = true
      @write_upstream = write_upstream
      start_sync(heartbeat)
    end

    # -------------------------------------------------------------------------
    # Upstream write overrides (only active when write_upstream: true)
    # -------------------------------------------------------------------------

    # In upstream mode: writes to the remote and blocks until the change has
    # replicated to local. In local mode: delegates to `super` (local SQLite).
    def put(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
      return super unless @write_upstream
      # Capture local seq BEFORE the remote write so we don't miss the
      # replicated change if the pull fiber delivers it quickly.
      since = @adapter.info[:update_seq].to_s
      result = @remote.put(doc)
      wait_for_local(result[:id], result[:rev], since: since)
      result
    end

    # In upstream mode: removes on the remote and blocks until the tombstone
    # has replicated to local. In local mode: delegates to `super`.
    def remove(id : String, rev : String) : NamedTuple(ok: Bool)
      return super unless @write_upstream
      since = @adapter.info[:update_seq].to_s
      result = @remote.remove(id, rev)
      wait_for_local_deletion(id, since: since)
      result
    end

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    # Stops background sync fibers and closes both the local and remote adapters.
    # Fibers complete their current in-progress `replicate` call before stopping.
    def close
      @running = false
      super # closes local SQLite
      @remote.close
    end

    # -------------------------------------------------------------------------
    # Background sync
    # -------------------------------------------------------------------------

    private def start_sync(heartbeat : Int32)
      spawn_sync_fiber("push", @adapter, @remote.adapter, heartbeat)
      spawn_sync_fiber("pull", @remote.adapter, @adapter, heartbeat)
    end

    private def spawn_sync_fiber(direction : String, source : Adapter, target : Adapter, heartbeat : Int32)
      remote_adapter = @remote.adapter
      spawn do
        current_seq = "0"
        while @running
          begin
            source.changes_feed(since: current_seq, heartbeat: heartbeat) do |change|
              current_seq = change["seq"]?.try(&.as_s?) || current_seq
              break # one change is enough to trigger a replication run
            end
          rescue ex : Exception
            notify_error(direction, ex)
            next
          end
          next unless @running
          begin
            session = Replication::Replicator.new(source, target,
              checkpoint_store: remote_adapter).replicate
            if session.ok?
              # Guard: don't regress to "0" on a no-op run. Session#last_seq
              # stays at "0" when the changes batch was empty and record_batch
              # was never called; current_seq already holds the triggering seq.
              seq = session.last_seq
              current_seq = seq unless seq == "0" && current_seq != "0"
            else
              notify_error(direction, Exception.new(session.error || "replication failed"))
            end
          rescue ex : Exception
            notify_error(direction, ex)
          end
        end
      end
    end

    private def notify_error(direction : String, ex : Exception)
      @on_sync_error.try(&.call(direction, ex))
    end

    # -------------------------------------------------------------------------
    # Upstream-write helpers
    # -------------------------------------------------------------------------

    # Polls the local Changes API (starting from *since*) until *id* appears
    # at exactly *rev*, or until the timeout elapses.
    # Using `changes` tracks sequence progress so we don't re-scan seen entries.
    # Calls via `self` (inherited from `Database`) to pick up the `limit: nil` default.
    private def wait_for_local(id : String, rev : String, since : String)
      deadline = Time.instant + UPSTREAM_WAIT_TIMEOUT
      current = since
      while Time.instant < deadline
        result = changes(since: current, include_docs: false)
        result[:results].each do |change|
          if change["id"]?.try(&.as_s?) == id
            begin
              return if get(id).rev == rev
            rescue NotFound
            end
          end
        end
        current = result[:last_seq]
        sleep UPSTREAM_WAIT_INTERVAL
      end
      raise Error.new("Timeout waiting for #{id}@#{rev} to replicate locally")
    end

    # Polls the local Changes API until the deletion tombstone for *id* appears
    # (`_deleted: true` in a change entry), or until the timeout elapses.
    private def wait_for_local_deletion(id : String, since : String)
      deadline = Time.instant + UPSTREAM_WAIT_TIMEOUT
      current = since
      while Time.instant < deadline
        result = changes(since: current, include_docs: false)
        result[:results].each do |change|
          if change["id"]?.try(&.as_s?) == id
            return if change["deleted"]?.try(&.as_bool?) == true
          end
        end
        current = result[:last_seq]
        sleep UPSTREAM_WAIT_INTERVAL
      end
      raise Error.new("Timeout waiting for deletion of #{id} to replicate locally")
    end
  end
end
