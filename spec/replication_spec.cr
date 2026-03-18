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
