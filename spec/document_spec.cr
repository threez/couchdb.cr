require "./spec_helper"

# Subclass used in the subclassing spec below
class DocSpecNote < CouchDB::Document
  property title : String = ""
  property body : String = ""
end

describe CouchDB::Document do
  describe "construction and defaults" do
    it "has empty id, nil rev, nil deleted by default" do
      doc = CouchDB::Document.new
      doc.id.should eq("")
      doc.rev.should be_nil
      doc.deleted?.should be_false
    end
  end

  describe "hash-style access" do
    it "routes _id to the typed field" do
      doc = CouchDB::Document.new
      doc["_id"] = JSON::Any.new("foo")
      doc.id.should eq("foo")
      doc["_id"].as_s.should eq("foo")
    end

    it "routes _rev to the typed field" do
      doc = CouchDB::Document.new
      doc["_rev"] = JSON::Any.new("1-abc")
      doc.rev.should eq("1-abc")
      doc["_rev"].as_s.should eq("1-abc")
    end

    it "routes _deleted to the typed field" do
      doc = CouchDB::Document.new
      doc["_deleted"] = JSON::Any.new(true)
      doc.deleted?.should be_true
    end

    it "stores arbitrary keys in json_unmapped" do
      doc = CouchDB::Document.new
      doc["msg"] = JSON::Any.new("hello")
      doc["msg"].as_s.should eq("hello")
      doc["msg"]?.try(&.as_s).should eq("hello")
    end

    it "returns nil for missing keys via []?" do
      doc = CouchDB::Document.new
      doc["_rev"]?.should be_nil
      doc["missing"]?.should be_nil
    end
  end

  describe "JSON serialization" do
    it "omits _rev and _deleted when nil" do
      doc = CouchDB::Document.new
      doc.id = "foo"
      json = doc.to_json
      json.should contain(%("_id":"foo"))
      json.should_not contain("_rev")
      json.should_not contain("_deleted")
    end

    it "emits _deleted only when true" do
      doc = CouchDB::Document.new
      doc.id = "foo"
      doc.deleted = true
      doc.to_json.should contain(%("_deleted":true))
    end

    it "round-trips through JSON" do
      doc = CouchDB::Document.new
      doc.id = "foo"
      doc["msg"] = JSON::Any.new("hello")
      parsed = CouchDB::Document.from_json(doc.to_json)
      parsed.id.should eq("foo")
      parsed["msg"].as_s.should eq("hello")
    end

    it "deserializes CouchDB-style JSON" do
      json = %({"_id":"abc","_rev":"1-xyz","title":"Test"})
      doc = CouchDB::Document.from_json(json)
      doc.id.should eq("abc")
      doc.rev.should eq("1-xyz")
      doc["title"].as_s.should eq("Test")
    end
  end

  describe "#next_rev" do
    it "generates rev 1-<hash> for new doc" do
      doc = CouchDB::Document.new
      doc.id = "foo"
      doc["msg"] = JSON::Any.new("hello")
      doc.next_rev.should match(/^1-[a-f0-9]{32}$/)
    end

    it "increments the rev counter" do
      doc = CouchDB::Document.new
      doc.id = "foo"
      doc.rev = "3-abc"
      doc["msg"] = JSON::Any.new("hello")
      doc.next_rev.should match(/^4-[a-f0-9]{32}$/)
    end
  end

  describe "#deleted?" do
    it "returns false by default" do
      CouchDB::Document.new.deleted?.should be_false
    end

    it "returns true when deleted is set to true" do
      doc = CouchDB::Document.new
      doc.deleted = true
      doc.deleted?.should be_true
    end
  end

  describe "subclassing" do
    it "supports strongly-typed subclass properties" do
      note = DocSpecNote.new
      note.id = "note-1"
      note.title = "Hello"
      note.body = "World"

      parsed = DocSpecNote.from_json(note.to_json)
      parsed.id.should eq("note-1")
      parsed.title.should eq("Hello")
      parsed.body.should eq("World")
    end
  end
end

describe CouchDB::DocumentHelper do
  describe ".next_rev" do
    it "generates rev 1-<hash> for new doc" do
      doc = CouchDB::Document.new
      doc.id = "foo"
      doc["msg"] = JSON::Any.new("hello")
      rev = CouchDB::DocumentHelper.next_rev(doc)
      rev.should match(/^1-[a-f0-9]{32}$/)
    end

    it "increments the rev counter" do
      doc = CouchDB::Document.new
      doc.id = "foo"
      doc.rev = "3-abc"
      doc["msg"] = JSON::Any.new("hello")
      rev = CouchDB::DocumentHelper.next_rev(doc)
      rev.should match(/^4-[a-f0-9]{32}$/)
    end
  end

  describe ".rev_num" do
    it "parses the integer from a rev string" do
      CouchDB::DocumentHelper.rev_num("7-deadbeef").should eq(7)
    end
  end

  describe ".deleted?" do
    it "returns false when deleted is nil" do
      doc = CouchDB::Document.new
      doc.id = "x"
      CouchDB::DocumentHelper.deleted?(doc).should be_false
    end

    it "returns true when deleted is true" do
      doc = CouchDB::Document.new
      doc.id = "x"
      doc.deleted = true
      CouchDB::DocumentHelper.deleted?(doc).should be_true
    end
  end
end
