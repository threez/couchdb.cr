require "./spec_helper"

# Subclass used in typed-get spec
class ReplicationSpecNote < CouchDB::Document
  property title : String = ""
end

private def mem_db : CouchDB::Adapter::SQLite
  CouchDB::Adapter::SQLite.new(":memory:")
end

private def make_doc(id : String, **fields) : CouchDB::Document
  doc = CouchDB::Document.new
  doc.id = id
  fields.each { |k, v| doc[k.to_s] = JSON::Any.new(v) }
  doc
end

describe CouchDB::Replication::Replicator do
  it "replicates docs from source to target" do
    source = mem_db
    target = mem_db

    source.put(make_doc("doc1", msg: "hello"))
    source.put(make_doc("doc2", msg: "world"))

    session = CouchDB::Replication::Replicator.new(source, target).replicate
    session.ok?.should be_true
    session.docs_written.should eq(2)

    target.get("doc1")["msg"].as_s.should eq("hello")
    target.get("doc2")["msg"].as_s.should eq("world")
  end

  it "is idempotent — second replication does no work" do
    source = mem_db
    target = mem_db

    source.put(make_doc("x"))
    CouchDB::Replication::Replicator.new(source, target).replicate

    session2 = CouchDB::Replication::Replicator.new(source, target).replicate
    session2.ok?.should be_true
    session2.docs_written.should eq(0)
  end

  it "replicates only new docs after checkpoint" do
    source = mem_db
    target = mem_db

    source.put(make_doc("a"))
    CouchDB::Replication::Replicator.new(source, target).replicate

    source.put(make_doc("b"))
    session = CouchDB::Replication::Replicator.new(source, target).replicate
    session.docs_written.should eq(1)
    target.get("b").id.should eq("b")
  end
end

describe CouchDB::Database do
  it "auto-selects SQLite adapter for non-URL path" do
    db = CouchDB::Database.new(":memory:")
    db.adapter.should be_a(CouchDB::Adapter::SQLite)
  end

  it "supports put/get through facade" do
    db = CouchDB::Database.new(":memory:")
    doc = CouchDB::Document.new
    doc.id = "z"
    doc["x"] = JSON::Any.new("1")
    r = db.put(doc)
    r[:ok].should be_true
    db.get("z")["x"].as_s.should eq("1")
  end

  it "supports typed get with subclass" do
    db = CouchDB::Database.new(":memory:")
    note = ReplicationSpecNote.new
    note.id = "n1"
    note.title = "Hello"
    db.put(note)

    retrieved = db.get("n1", as: ReplicationSpecNote)
    retrieved.title.should eq("Hello")
    retrieved.id.should eq("n1")
  end

  it "sync pulls and pushes between two local dbs" do
    local = CouchDB::Database.new(":memory:")
    remote = CouchDB::Database.new(":memory:")

    local_doc = CouchDB::Document.new
    local_doc.id = "local_doc"
    local.put(local_doc)

    remote_doc = CouchDB::Document.new
    remote_doc.id = "remote_doc"
    remote.put(remote_doc)

    local.sync(remote)

    local.get("remote_doc").id.should eq("remote_doc")
    remote.get("local_doc").id.should eq("local_doc")
  end
end

describe "filtered replication" do
  it "replicates only the listed doc_ids" do
    source = CouchDB::Database.new(":memory:")
    target = CouchDB::Database.new(":memory:")

    source.put(make_doc("keep", msg: "yes"))
    source.put(make_doc("skip", msg: "no"))

    session = source.replicate_to(target, doc_ids: ["keep"])
    session.ok?.should be_true
    session.docs_written.should eq(1)
    target.get("keep")["msg"].as_s.should eq("yes")
    expect_raises(CouchDB::NotFound) { target.get("skip") }
  end

  it "skips docs not in doc_ids" do
    source = CouchDB::Database.new(":memory:")
    target = CouchDB::Database.new(":memory:")

    source.put(make_doc("a", val: "1"))
    source.put(make_doc("b", val: "2"))
    source.put(make_doc("c", val: "3"))

    source.replicate_to(target, doc_ids: ["a", "c"])
    target.get("a").id.should eq("a")
    target.get("c").id.should eq("c")
    expect_raises(CouchDB::NotFound) { target.get("b") }
  end

  it "replicates only docs passing the filter proc" do
    source = CouchDB::Database.new(":memory:")
    target = CouchDB::Database.new(":memory:")

    source.put(make_doc("p1", score: 10_i64))
    source.put(make_doc("p2", score: 5_i64))
    source.put(make_doc("p3", score: 8_i64))

    filter = ->(doc : CouchDB::Document) {
      score = doc["score"]?.try(&.as_i64?) || 0_i64
      score >= 8
    }
    source.replicate_to(target, filter: filter)
    target.get("p1").id.should eq("p1")
    target.get("p3").id.should eq("p3")
    expect_raises(CouchDB::NotFound) { target.get("p2") }
  end

  it "skips docs that fail the filter proc" do
    source = CouchDB::Database.new(":memory:")
    target = CouchDB::Database.new(":memory:")

    source.put(make_doc("x", active: true))
    source.put(make_doc("y", active: false))

    filter = ->(doc : CouchDB::Document) {
      doc["active"]?.try(&.as_bool?) == true
    }
    session = source.replicate_to(target, filter: filter)
    session.docs_written.should eq(1)
    target.get("x").id.should eq("x")
    expect_raises(CouchDB::NotFound) { target.get("y") }
  end

  it "replicates only docs matching the selector" do
    source = CouchDB::Database.new(":memory:")
    target = CouchDB::Database.new(":memory:")

    source.put(make_doc("m1", type: "note"))
    source.put(make_doc("m2", type: "task"))
    source.put(make_doc("m3", type: "note"))

    selector = JSON.parse(%({"type": "note"}))
    source.replicate_to(target, selector: selector)
    target.get("m1").id.should eq("m1")
    target.get("m3").id.should eq("m3")
    expect_raises(CouchDB::NotFound) { target.get("m2") }
  end

  it "composes doc_ids with filter proc" do
    source = CouchDB::Database.new(":memory:")
    target = CouchDB::Database.new(":memory:")

    source.put(make_doc("d1", score: 10_i64))
    source.put(make_doc("d2", score: 10_i64))
    source.put(make_doc("d3", score: 1_i64))

    filter = ->(doc : CouchDB::Document) {
      score = doc["score"]?.try(&.as_i64?) || 0_i64
      score >= 8
    }
    # doc_ids restricts to d1+d2, filter further restricts to score >= 8
    # d3 excluded by doc_ids, d2 included by both, d1 included by both
    source.replicate_to(target, doc_ids: ["d1", "d2"], filter: filter)
    target.get("d1").id.should eq("d1")
    target.get("d2").id.should eq("d2")
    expect_raises(CouchDB::NotFound) { target.get("d3") }
  end
end
