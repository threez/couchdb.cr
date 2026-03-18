require "base64"
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

    it "filters by startkey" do
      db = tmp_db
      %w[apple banana cherry].each { |id| db.put(make_doc(id)) }
      ids = db.all_docs(startkey: "banana")[:rows].map(&.["id"].as_s)
      ids.should eq(%w[banana cherry])
    end

    it "filters by endkey" do
      db = tmp_db
      %w[apple banana cherry].each { |id| db.put(make_doc(id)) }
      ids = db.all_docs(endkey: "banana")[:rows].map(&.["id"].as_s)
      ids.should eq(%w[apple banana])
    end

    it "filters by startkey and endkey" do
      db = tmp_db
      %w[apple banana cherry date].each { |id| db.put(make_doc(id)) }
      ids = db.all_docs(startkey: "banana", endkey: "cherry")[:rows].map(&.["id"].as_s)
      ids.should eq(%w[banana cherry])
    end

    it "returns empty rows when range has no matches" do
      db = tmp_db
      db.put(make_doc("apple"))
      result = db.all_docs(startkey: "z")
      result[:rows].should be_empty
      result[:total_rows].should eq(1_i64)
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

  describe "#get_attachment, #put_attachment, #delete_attachment" do
    it "stores and retrieves an attachment" do
      db = tmp_db
      rev = db.put(make_doc("att-doc"))[:rev]
      data = "hello attachment".to_slice
      result = db.put_attachment("att-doc", "readme.txt", rev, data, "text/plain")
      result[:ok].should be_true

      att = db.get_attachment("att-doc", "readme.txt")
      att[:content_type].should eq("text/plain")
      String.new(att[:data]).should eq("hello attachment")
    end

    it "updates _attachments stub in the document" do
      db = tmp_db
      rev = db.put(make_doc("att-meta"))[:rev]
      db.put_attachment("att-meta", "file.bin", rev, Bytes[1, 2, 3], "application/octet-stream")
      doc = db.get("att-meta")
      stubs = doc["_attachments"].as_h
      stubs["file.bin"]["content_type"].as_s.should eq("application/octet-stream")
      stubs["file.bin"]["length"].as_i.should eq(3)
    end

    it "raises NotFound for missing attachment" do
      db = tmp_db
      db.put(make_doc("att-nope"))
      expect_raises(CouchDB::NotFound) { db.get_attachment("att-nope", "no.txt") }
    end

    it "raises Conflict when rev is wrong" do
      db = tmp_db
      db.put(make_doc("att-conflict"))
      expect_raises(CouchDB::Conflict) do
        db.put_attachment("att-conflict", "x.txt", "0-bad", Bytes[1], "text/plain")
      end
    end

    it "deletes an attachment" do
      db = tmp_db
      rev = db.put(make_doc("att-del"))[:rev]
      r2 = db.put_attachment("att-del", "bye.txt", rev, "bye".to_slice, "text/plain")
      db.delete_attachment("att-del", "bye.txt", r2[:rev])
      expect_raises(CouchDB::NotFound) { db.get_attachment("att-del", "bye.txt") }
    end

    it "extracts inline attachment data from a put document" do
      db = tmp_db
      doc = make_doc("inline-att")
      doc["_attachments"] = JSON::Any.new({
        "hello.txt" => JSON::Any.new({
          "content_type" => JSON::Any.new("text/plain"),
          "data"         => JSON::Any.new(Base64.strict_encode("inline content")),
          "length"       => JSON::Any.new(14_i64),
        } of String => JSON::Any),
      } of String => JSON::Any)
      db.put(doc)

      att = db.get_attachment("inline-att", "hello.txt")
      String.new(att[:data]).should eq("inline content")
      att[:content_type].should eq("text/plain")
    end

    it "stores a stub (not inline data) in the document body after extracting" do
      db = tmp_db
      doc = make_doc("inline-stub")
      doc["_attachments"] = JSON::Any.new({
        "f.bin" => JSON::Any.new({
          "content_type" => JSON::Any.new("application/octet-stream"),
          "data"         => JSON::Any.new(Base64.strict_encode("abc")),
          "length"       => JSON::Any.new(3_i64),
        } of String => JSON::Any),
      } of String => JSON::Any)
      db.put(doc)

      stored = db.get("inline-stub")
      stub = stored["_attachments"].as_h["f.bin"].as_h
      stub["stub"].as_bool.should be_true
      stub.has_key?("data").should be_false
    end
  end
end
