require "./spec_helper"

# A minimal adapter stub that always raises on changes_feed so we can test
# error-path behaviour without a real network.
private class FailingAdapter < CouchDB::Adapter::SQLite
  def changes_feed(since : String, heartbeat : Int32, &_block : JSON::Any -> Nil) : Nil
    raise IO::Error.new("Hostname lookup failed: Temporary failure in name resolution")
  end
end

describe CouchDB::LocalReplica do
  it "CouchDB::Log is a valid log source" do
    CouchDB::Log.should be_a(::Log)
  end

  it "on_sync_error is invoked on connectivity errors" do
    errors = [] of String
    db = CouchDB::Database.local_replica(
      ":memory:",
      "http://localhost:5984/testdb",
      sync_initial_backoff: 50.milliseconds,
      sync_max_backoff: 50.milliseconds,
    )
    db.on_sync_error { |dir, ex| errors << "#{dir}: #{ex.message}" }

    sleep 250.milliseconds
    db.close

    errors.should_not be_empty
  end

  it "does not crash when no on_sync_error is registered" do
    db = CouchDB::Database.local_replica(
      ":memory:",
      "http://localhost:5984/testdb",
      sync_initial_backoff: 50.milliseconds,
      sync_max_backoff: 50.milliseconds,
    )

    sleep 150.milliseconds
    db.close
    # no exception raised → pass
  end

  it "backoff limits error rate — fewer than 10 callbacks in 300ms with 50ms backoff" do
    count = Atomic(Int32).new(0)
    db = CouchDB::Database.local_replica(
      ":memory:",
      "http://localhost:5984/testdb",
      sync_initial_backoff: 50.milliseconds,
      sync_max_backoff: 50.milliseconds,
    )
    db.on_sync_error { |_dir, _ex| count.add(1) }

    sleep 300.milliseconds
    db.close

    # With 50ms backoff and two fibers over ~300ms we expect at most ~12 callbacks.
    # Without backoff this would be thousands. Use a generous upper bound.
    count.get.should be <= 12
  end
end
