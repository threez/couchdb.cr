require "base64"
require "http/client"
require "uri"
require "./spec_helper"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

private BASE_URL = ENV["COUCHDB_URL"]? || ""
private TEST_DB  = "couchdb_cr_test"
private DB_URL   = "#{BASE_URL}/#{TEST_DB}"

private def e2e_headers : HTTP::Headers
  uri = URI.parse(BASE_URL)
  headers = HTTP::Headers{"Accept" => "application/json"}
  if user = uri.user
    headers["Authorization"] = "Basic #{Base64.strict_encode("#{user}:#{uri.password}")}"
  end
  headers
end

private def create_test_db
  HTTP::Client.put(DB_URL, headers: e2e_headers)
end

private def drop_test_db
  HTTP::Client.delete(DB_URL, headers: e2e_headers)
end

private def http_db : CouchDB::Database
  CouchDB::Database.new(DB_URL)
end

private def make_doc(id : String, **fields) : CouchDB::Document
  doc = CouchDB::Document.new
  doc.id = id
  fields.each { |k, v| doc[k.to_s] = JSON::Any.new(v) }
  doc
end

# ---------------------------------------------------------------------------
# Guard — skip everything when no server is configured
# ---------------------------------------------------------------------------

if BASE_URL.empty?
  pending "Set COUCHDB_URL (e.g. http://admin:secret@localhost:7070) to run HTTP e2e tests"
else
  Spec.before_suite { create_test_db }
  Spec.after_suite { drop_test_db }

  # -------------------------------------------------------------------------
  # Adapter::HTTP — mirrors adapter_sqlite_spec.cr
  # -------------------------------------------------------------------------

  describe CouchDB::Adapter::HTTP do
    describe "#info" do
      it "returns db_name and numeric counts" do
        info = http_db.info
        info[:db_name].should eq(TEST_DB)
        info[:doc_count].should be_a(Int64)
        info[:update_seq].should be_a(Int64)
      end
    end

    describe "#put and #get" do
      it "stores and retrieves a document" do
        db = http_db
        result = db.put(make_doc("http-hello", msg: "world"))
        result[:ok].should be_true
        result[:id].should eq("http-hello")
        result[:rev].should match(/^1-[a-f0-9]{32}$/)

        fetched = db.get("http-hello")
        fetched.id.should eq("http-hello")
        fetched["msg"].as_s.should eq("world")
        fetched.rev.should eq(result[:rev])
      end

      it "raises NotFound for a missing document" do
        expect_raises(CouchDB::NotFound) { http_db.get("http-no-such-doc-xyz") }
      end

      it "raises Conflict when rev is wrong" do
        db = http_db
        db.put(make_doc("http-conflict-doc", v: "1"))
        bad = make_doc("http-conflict-doc", v: "2")
        bad.rev = "0-bad"
        expect_raises(CouchDB::Conflict) { db.put(bad) }
      end

      it "updates a document with the correct rev" do
        db = http_db
        r1 = db.put(make_doc("http-update-doc", v: "1"))
        doc2 = make_doc("http-update-doc", v: "2")
        doc2.rev = r1[:rev]
        r2 = db.put(doc2)
        r2[:rev].should match(/^2-[a-f0-9]{32}$/)
        db.get("http-update-doc")["v"].as_s.should eq("2")
      end
    end

    describe "#remove" do
      it "marks a document as deleted" do
        db = http_db
        rev = db.put(make_doc("http-del-me"))[:rev]
        db.remove("http-del-me", rev)
        expect_raises(CouchDB::NotFound) { db.get("http-del-me") }
      end
    end

    describe "#bulk_docs" do
      it "inserts multiple documents" do
        db = http_db
        docs = (1..3).map { |i| make_doc("http-bulk-#{i}", n: i.to_s) }
        results = db.bulk_docs(docs)
        results.size.should eq(3)
        results.all?(&.[:ok]).should be_true
      end

      it "stores docs with new_edits: false (replication path)" do
        db = http_db
        doc = make_doc("http-rep-doc", msg: "hi")
        doc.rev = "5-abcdefabcdefabcdefabcdefabcdefab"
        results = db.bulk_docs([doc], new_edits: false)
        results.first[:ok].should be_true
        results.first[:rev].should eq("5-abcdefabcdefabcdefabcdefabcdefab")
      end
    end

    describe "#all_docs" do
      it "returns rows for non-deleted docs" do
        db = http_db
        db.put(make_doc("http-all-a"))
        db.put(make_doc("http-all-b"))
        result = db.all_docs
        result[:total_rows].should be >= 2_i64
      end

      it "includes doc bodies when include_docs: true" do
        db = http_db
        db.put(make_doc("http-all-include", val: "check"))
        result = db.all_docs(include_docs: true)
        found = result[:rows].any? do |row|
          row["doc"]?.try(&.["val"]?.try(&.as_s?)) == "check"
        end
        found.should be_true
      end

      it "filters by startkey and endkey" do
        db = http_db
        %w[http-range-a http-range-b http-range-c].each { |id| db.put(make_doc(id)) }
        result = db.all_docs(startkey: "http-range-b", endkey: "http-range-c")
        ids = result[:rows].map(&.["id"].as_s)
        ids.should contain("http-range-b")
        ids.should contain("http-range-c")
        ids.should_not contain("http-range-a")
      end
    end

    describe "#changes" do
      it "returns changes since a given seq" do
        db = http_db
        db.put(make_doc("http-changes-a"))
        result = db.changes(since: "0")
        result[:results].size.should be > 0
      end

      it "returns only new changes since the last seq" do
        db = http_db
        db.put(make_doc("http-changes-b1"))
        seq = db.changes(since: "0")[:last_seq]
        db.put(make_doc("http-changes-b2"))
        result = db.changes(since: seq)
        result[:results].size.should eq(1)
        result[:results].first["id"].as_s.should eq("http-changes-b2")
      end
    end

    describe "#changes_feed" do
      it "streams changes via feed=continuous" do
        db = http_db
        baseline = db.changes(since: "0")[:last_seq]
        db.put(make_doc("http-feed-a"))

        seen = [] of JSON::Any
        db.changes_feed(since: baseline, heartbeat: 1000) do |change|
          seen << change
          break
        end

        seen.size.should eq(1)
        seen.first["id"].as_s.should eq("http-feed-a")
      end
    end

    describe "#revs_diff" do
      it "returns missing revisions" do
        db = http_db
        rev = db.put(make_doc("http-rdiff"))[:rev]
        diff = db.revs_diff({"http-rdiff" => [rev, "99-missing"]})
        diff["http-rdiff"][:missing].should contain("99-missing")
        diff["http-rdiff"][:missing].should_not contain(rev)
      end

      it "returns nothing when all revs exist" do
        db = http_db
        rev = db.put(make_doc("http-rdiff-exists"))[:rev]
        db.revs_diff({"http-rdiff-exists" => [rev]}).should be_empty
      end
    end

    describe "#bulk_get" do
      it "fetches specific id/rev pairs" do
        db = http_db
        rev = db.put(make_doc("http-bget", v: "hello"))[:rev]
        docs = db.bulk_get([{id: "http-bget", rev: rev}])
        docs.size.should eq(1)
        docs.first["v"].as_s.should eq("hello")
      end
    end

    describe "#get_local and #put_local" do
      it "stores and retrieves local docs" do
        db = http_db
        doc = CouchDB::Document.new
        doc.id = "_local/http-ckpt1"
        doc["last_seq"] = JSON::Any.new("99")
        db.put_local(doc)
        fetched = db.get_local("_local/http-ckpt1")
        fetched["last_seq"].as_s.should eq("99")
      end

      it "raises NotFound for missing local doc" do
        expect_raises(CouchDB::NotFound) { http_db.get_local("_local/http-nope") }
      end
    end

    describe "#get_attachment, #put_attachment, #delete_attachment" do
      it "stores and retrieves an attachment" do
        db = http_db
        rev = db.put(make_doc("http-att-doc"))[:rev]
        data = "hello".to_slice
        result = db.put_attachment("http-att-doc", "note.txt", rev, data, "text/plain")
        result[:ok].should be_true

        att = db.get_attachment("http-att-doc", "note.txt")
        String.new(att[:data]).should eq("hello")
        att[:content_type].should start_with("text/plain")
      end

      it "deletes an attachment" do
        db = http_db
        rev = db.put(make_doc("http-att-del"))[:rev]
        r2 = db.put_attachment("http-att-del", "bye.txt", rev, "bye".to_slice, "text/plain")
        db.delete_attachment("http-att-del", "bye.txt", r2[:rev])
        expect_raises(CouchDB::NotFound) { db.get_attachment("http-att-del", "bye.txt") }
      end
    end
  end

  # -------------------------------------------------------------------------
  # HTTP ↔ SQLite replication
  # -------------------------------------------------------------------------

  describe "HTTP ↔ SQLite replication" do
    it "replicates from HTTP to SQLite" do
      remote = http_db
      local = CouchDB::Database.new(":memory:")

      remote.put(make_doc("rep-http-to-sqlite"))
      session = local.replicate_from(remote)
      session.ok?.should be_true
      session.docs_written.should be >= 1
      local.get("rep-http-to-sqlite").id.should eq("rep-http-to-sqlite")
    end

    it "replicates from SQLite to HTTP" do
      local = CouchDB::Database.new(":memory:")
      remote = http_db

      local.put(make_doc("rep-sqlite-to-http"))
      session = local.replicate_to(remote)
      session.ok?.should be_true
      session.docs_written.should be >= 1
      remote.get("rep-sqlite-to-http").id.should eq("rep-sqlite-to-http")
    end

    it "bidirectional sync" do
      local = CouchDB::Database.new(":memory:")
      remote = http_db

      local.put(make_doc("rep-sync-local-doc"))
      remote.put(make_doc("rep-sync-remote-doc"))

      local.sync(remote)

      local.get("rep-sync-remote-doc").id.should eq("rep-sync-remote-doc")
      remote.get("rep-sync-local-doc").id.should eq("rep-sync-local-doc")
    end

    it "replicates attachments from HTTP to SQLite" do
      remote = http_db
      rev = remote.put(make_doc("rep-att-src"))[:rev]
      remote.put_attachment("rep-att-src", "data.txt", rev, "replicated".to_slice, "text/plain")

      local = CouchDB::Database.new(":memory:")
      local.replicate_from(remote)

      att = local.get_attachment("rep-att-src", "data.txt")
      String.new(att[:data]).should eq("replicated")
    end
  end
end
