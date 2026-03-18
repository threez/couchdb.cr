require "./spec_helper"

private def tmp_db : CouchDB::Adapter::SQLite
  CouchDB::Adapter::SQLite.new(":memory:")
end

private def make_doc(id : String, **fields) : CouchDB::Document
  doc = CouchDB::Document.new
  doc.id = id
  fields.each { |k, v| doc[k.to_s] = JSON::Any.new(v) }
  doc
end

describe CouchDB::Adapter::SQLite do
  describe "#info" do
    it "returns zero counts on empty db" do
      db = tmp_db
      info = db.info
      info[:doc_count].should eq(0_i64)
      info[:update_seq].should eq(0_i64)
      info[:db_name].should start_with("sqlite")
    end
  end

  describe "#put and #get" do
    it "stores and retrieves a document" do
      db = tmp_db
      doc = make_doc("hello", msg: "world")
      result = db.put(doc)
      result[:ok].should be_true
      result[:id].should eq("hello")
      result[:rev].should match(/^1-[a-f0-9]{32}$/)

      fetched = db.get("hello")
      fetched.id.should eq("hello")
      fetched["msg"].as_s.should eq("world")
      fetched.rev.should eq(result[:rev])
    end

    it "raises NotFound for missing document" do
      db = tmp_db
      expect_raises(CouchDB::NotFound) { db.get("missing") }
    end

    it "raises Conflict when rev is wrong" do
      db = tmp_db
      db.put(make_doc("x", v: "1"))
      bad = make_doc("x", v: "2")
      bad.rev = "0-bad"
      expect_raises(CouchDB::Conflict) { db.put(bad) }
    end

    it "updates a document with correct rev" do
      db = tmp_db
      r1 = db.put(make_doc("x", v: "1"))
      doc2 = make_doc("x", v: "2")
      doc2.rev = r1[:rev]
      r2 = db.put(doc2)
      r2[:rev].should match(/^2-[a-f0-9]{32}$/)
      db.get("x")["v"].as_s.should eq("2")
    end
  end

  describe "#remove" do
    it "marks a document as deleted" do
      db = tmp_db
      r = db.put(make_doc("del_me"))
      db.remove("del_me", r[:rev])
      expect_raises(CouchDB::NotFound) { db.get("del_me") }
    end
  end

  describe "#info doc_count" do
    it "counts only non-deleted docs" do
      db = tmp_db
      r = db.put(make_doc("a"))
      db.put(make_doc("b"))
      db.info[:doc_count].should eq(2_i64)
      db.remove("a", r[:rev])
      db.info[:doc_count].should eq(1_i64)
    end
  end

  describe "#bulk_docs" do
    it "inserts multiple documents" do
      db = tmp_db
      docs = (1..3).map { |i| make_doc("doc#{i}", n: i.to_s) }
      results = db.bulk_docs(docs)
      results.size.should eq(3)
      results.all?(&.[:ok]).should be_true
    end

    it "stores docs with new_edits: false (replication path)" do
      db = tmp_db
      doc = make_doc("rep1", msg: "hi")
      doc.rev = "5-abcdefabcdefabcdefabcdefabcdefab"
      results = db.bulk_docs([doc], new_edits: false)
      results.first[:ok].should be_true
      results.first[:rev].should eq("5-abcdefabcdefabcdefabcdefabcdefab")
    end
  end

  describe "#all_docs" do
    it "returns rows for all non-deleted docs" do
      db = tmp_db
      db.put(make_doc("a"))
      db.put(make_doc("b"))
      result = db.all_docs
      result[:total_rows].should eq(2_i64)
      result[:rows].size.should eq(2)
    end

    it "includes doc bodies when include_docs: true" do
      db = tmp_db
      db.put(make_doc("x", val: "1"))
      result = db.all_docs(include_docs: true)
      result[:rows].first["doc"]["val"].as_s.should eq("1")
    end
  end

  describe "#changes" do
    it "returns changes since a given seq" do
      db = tmp_db
      db.put(make_doc("a"))
      db.put(make_doc("b"))
      result = db.changes(since: "0")
      result[:results].size.should eq(2)
      result[:last_seq].to_i.should be > 0
    end

    it "only returns new changes since last seq" do
      db = tmp_db
      db.put(make_doc("a"))
      seq = db.changes(since: "0")[:last_seq]
      db.put(make_doc("b"))
      result = db.changes(since: seq)
      result[:results].size.should eq(1)
      result[:results].first["id"].as_s.should eq("b")
    end
  end

  describe "#revs_diff" do
    it "returns missing revisions" do
      db = tmp_db
      r = db.put(make_doc("x"))
      diff = db.revs_diff({"x" => [r[:rev], "99-missing"]})
      diff["x"][:missing].should contain("99-missing")
      diff["x"][:missing].should_not contain(r[:rev])
    end

    it "returns nothing when all revs exist" do
      db = tmp_db
      r = db.put(make_doc("x"))
      diff = db.revs_diff({"x" => [r[:rev]]})
      diff.should be_empty
    end
  end

  describe "#bulk_get" do
    it "fetches specific id/rev pairs" do
      db = tmp_db
      r = db.put(make_doc("x", v: "hi"))
      docs = db.bulk_get([{id: "x", rev: r[:rev]}])
      docs.size.should eq(1)
      docs.first["v"].as_s.should eq("hi")
    end
  end

  describe "#get_local and #put_local" do
    it "stores and retrieves local docs" do
      db = tmp_db
      doc = CouchDB::Document.new
      doc.id = "_local/ckpt1"
      doc["last_seq"] = JSON::Any.new("42")
      db.put_local(doc)
      fetched = db.get_local("_local/ckpt1")
      fetched["last_seq"].as_s.should eq("42")
    end

    it "raises NotFound for missing local doc" do
      db = tmp_db
      expect_raises(CouchDB::NotFound) { db.get_local("_local/nope") }
    end
  end
end
