require "./spec_helper"

private def mem_db
  CouchDB::Database.new(":memory:")
end

private def seed(db, id, **fields)
  doc = CouchDB::Document.new
  doc.id = id
  fields.each { |k, v| doc[k.to_s] = JSON::Any.new(v) }
  db.put(doc)
end

private def seed_doc(db, id, &)
  doc = CouchDB::Document.new
  doc.id = id
  yield doc
  db.put(doc)
end

# Convenience: parse a JSON string into a selector.
private def sel(json_str : String) : JSON::Any
  JSON.parse(json_str)
end

describe CouchDB::Database do
  describe "#find" do
    it "empty selector {} matches all docs" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")

      result = db.find(sel("{}"))
      result[:docs].size.should eq(2)
    end

    it "implicit $eq on string field" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")

      result = db.find(sel(%({"type": "note"})))
      result[:docs].size.should eq(1)
      result[:docs].first["type"].as_s.should eq("note")
    end

    it "explicit $eq" do
      db = mem_db
      seed(db, "doc-1", score: 42_i64)
      seed(db, "doc-2", score: 99_i64)

      result = db.find(sel(%({"score": {"$eq": 42}})))
      result[:docs].size.should eq(1)
      result[:docs].first["score"].as_i64.should eq(42_i64)
    end

    it "$ne excludes matching doc" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")

      result = db.find(sel(%({"type": {"$ne": "note"}})))
      result[:docs].size.should eq(1)
      result[:docs].first["type"].as_s.should eq("task")
    end

    it "$lt / $lte numeric range" do
      db = mem_db
      seed(db, "doc-1", score: 10_i64)
      seed(db, "doc-2", score: 20_i64)
      seed(db, "doc-3", score: 30_i64)

      lt_result = db.find(sel(%({"score": {"$lt": 20}})))
      lt_result[:docs].size.should eq(1)
      lt_result[:docs].first["score"].as_i64.should eq(10_i64)

      lte_result = db.find(sel(%({"score": {"$lte": 20}})))
      lte_result[:docs].size.should eq(2)
    end

    it "$gt / $gte numeric range" do
      db = mem_db
      seed(db, "doc-1", score: 10_i64)
      seed(db, "doc-2", score: 20_i64)
      seed(db, "doc-3", score: 30_i64)

      gt_result = db.find(sel(%({"score": {"$gt": 20}})))
      gt_result[:docs].size.should eq(1)
      gt_result[:docs].first["score"].as_i64.should eq(30_i64)

      gte_result = db.find(sel(%({"score": {"$gte": 20}})))
      gte_result[:docs].size.should eq(2)
    end

    it "$exists: true — field present" do
      db = mem_db
      seed(db, "doc-1", email: "a@b.com")
      seed(db, "doc-2", name: "Bob")

      result = db.find(sel(%({"email": {"$exists": true}})))
      result[:docs].size.should eq(1)
      result[:docs].first["_id"].as_s.should eq("doc-1")
    end

    it "$exists: false — field absent" do
      db = mem_db
      seed(db, "doc-1", email: "a@b.com")
      seed(db, "doc-2", name: "Bob")

      result = db.find(sel(%({"email": {"$exists": false}})))
      result[:docs].size.should eq(1)
      result[:docs].first["_id"].as_s.should eq("doc-2")
    end

    it "$type filters by JSON type name" do
      db = mem_db
      seed(db, "doc-1", value: "hello")
      seed(db, "doc-2", value: 42_i64)

      str_result = db.find(sel(%({"value": {"$type": "string"}})))
      str_result[:docs].size.should eq(1)
      str_result[:docs].first["_id"].as_s.should eq("doc-1")

      num_result = db.find(sel(%({"value": {"$type": "number"}})))
      num_result[:docs].size.should eq(1)
      num_result[:docs].first["_id"].as_s.should eq("doc-2")
    end

    it "$in matches any of provided values" do
      db = mem_db
      seed(db, "doc-1", status: "active")
      seed(db, "doc-2", status: "pending")
      seed(db, "doc-3", status: "closed")

      result = db.find(sel(%({"status": {"$in": ["active", "pending"]}})))
      result[:docs].size.should eq(2)
    end

    it "$nin excludes all provided values" do
      db = mem_db
      seed(db, "doc-1", status: "active")
      seed(db, "doc-2", status: "pending")
      seed(db, "doc-3", status: "closed")

      result = db.find(sel(%({"status": {"$nin": ["active", "pending"]}})))
      result[:docs].size.should eq(1)
      result[:docs].first["status"].as_s.should eq("closed")
    end

    it "$all — array field contains all listed elements" do
      db = mem_db
      seed_doc(db, "doc-1") do |doc|
        doc["tags"] = JSON::Any.new([JSON::Any.new("a"), JSON::Any.new("b"), JSON::Any.new("c")])
      end
      seed_doc(db, "doc-2") do |doc|
        doc["tags"] = JSON::Any.new([JSON::Any.new("a"), JSON::Any.new("d")])
      end

      result = db.find(sel(%({"tags": {"$all": ["a", "b"]}})))
      result[:docs].size.should eq(1)
      result[:docs].first["_id"].as_s.should eq("doc-1")
    end

    it "$size — array field has exact length" do
      db = mem_db
      seed_doc(db, "doc-1") do |doc|
        doc["tags"] = JSON::Any.new([JSON::Any.new("a"), JSON::Any.new("b")])
      end
      seed_doc(db, "doc-2") do |doc|
        doc["tags"] = JSON::Any.new([JSON::Any.new("x")])
      end

      result = db.find(sel(%({"tags": {"$size": 2}})))
      result[:docs].size.should eq(1)
      result[:docs].first["_id"].as_s.should eq("doc-1")
    end

    it "$mod — integer modulo (even numbers)" do
      db = mem_db
      seed(db, "doc-1", n: 2_i64)
      seed(db, "doc-2", n: 3_i64)
      seed(db, "doc-3", n: 4_i64)

      result = db.find(sel(%({"n": {"$mod": [2, 0]}})))
      result[:docs].size.should eq(2)
      result[:docs].map { |d| d["n"].as_i64 }.sort.should eq([2_i64, 4_i64])
    end

    it "$regex — string matches pattern" do
      db = mem_db
      seed(db, "doc-1", name: "Alice")
      seed(db, "doc-2", name: "Bob")
      seed(db, "doc-3", name: "Albert")

      result = db.find(sel(%({"name": {"$regex": "^Al"}})))
      result[:docs].size.should eq(2)
    end

    it "$elemMatch — array element matches sub-selector" do
      db = mem_db
      seed_doc(db, "doc-1") do |doc|
        doc["items"] = JSON::Any.new([
          JSON::Any.new({"qty" => JSON::Any.new(5_i64)} of String => JSON::Any),
          JSON::Any.new({"qty" => JSON::Any.new(15_i64)} of String => JSON::Any),
        ])
      end
      seed_doc(db, "doc-2") do |doc|
        doc["items"] = JSON::Any.new([
          JSON::Any.new({"qty" => JSON::Any.new(1_i64)} of String => JSON::Any),
        ])
      end

      result = db.find(sel(%({"items": {"$elemMatch": {"qty": {"$gt": 10}}}})))
      result[:docs].size.should eq(1)
      result[:docs].first["_id"].as_s.should eq("doc-1")
    end

    it "$and — explicit combinator, equivalent to implicit multi-field" do
      db = mem_db
      seed(db, "doc-1", type: "note", status: "active")
      seed(db, "doc-2", type: "note", status: "closed")
      seed(db, "doc-3", type: "task", status: "active")

      explicit = db.find(sel(%({"$and": [{"type": "note"}, {"status": "active"}]})))
      implicit = db.find(sel(%({"type": "note", "status": "active"})))

      explicit[:docs].size.should eq(1)
      implicit[:docs].size.should eq(1)
      explicit[:docs].first["_id"].as_s.should eq(implicit[:docs].first["_id"].as_s)
    end

    it "$or — any condition matches" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")
      seed(db, "doc-3", type: "event")

      result = db.find(sel(%({"$or": [{"type": "note"}, {"type": "task"}]})))
      result[:docs].size.should eq(2)
    end

    it "$nor — no condition matches" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")
      seed(db, "doc-3", type: "event")

      result = db.find(sel(%({"$nor": [{"type": "note"}, {"type": "task"}]})))
      result[:docs].size.should eq(1)
      result[:docs].first["type"].as_s.should eq("event")
    end

    it "$not (field-level) — negates operator" do
      db = mem_db
      seed(db, "doc-1", score: 10_i64)
      seed(db, "doc-2", score: 50_i64)
      seed(db, "doc-3", score: 90_i64)

      result = db.find(sel(%({"score": {"$not": {"$gt": 20}}})))
      result[:docs].size.should eq(1)
      result[:docs].first["score"].as_i64.should eq(10_i64)
    end

    it "sort: ascending by single field" do
      db = mem_db
      seed(db, "doc-1", name: "Charlie")
      seed(db, "doc-2", name: "Alice")
      seed(db, "doc-3", name: "Bob")

      result = db.find(sel("{}"), sort: [JSON::Any.new("name")])
      result[:docs].map { |d| d["name"].as_s }.should eq(["Alice", "Bob", "Charlie"])
    end

    it "sort: descending by single field" do
      db = mem_db
      seed(db, "doc-1", name: "Charlie")
      seed(db, "doc-2", name: "Alice")
      seed(db, "doc-3", name: "Bob")

      result = db.find(sel("{}"), sort: [JSON.parse(%({"name": "desc"}))])
      result[:docs].map { |d| d["name"].as_s }.should eq(["Charlie", "Bob", "Alice"])
    end

    it "sort: multi-key (primary + secondary)" do
      db = mem_db
      seed(db, "doc-1", group: "a", rank: 2_i64)
      seed(db, "doc-2", group: "a", rank: 1_i64)
      seed(db, "doc-3", group: "b", rank: 1_i64)

      result = db.find(sel("{}"), sort: [JSON::Any.new("group"), JSON::Any.new("rank")])
      result[:docs].map { |d| d["_id"].as_s }.should eq(["doc-2", "doc-1", "doc-3"])
    end

    it "fields: projection — result contains only listed keys" do
      db = mem_db
      seed(db, "doc-1", name: "Alice", age: 30_i64, city: "NYC")

      result = db.find(sel("{}"), fields: ["name", "age"])
      doc = result[:docs].first
      doc.as_h.has_key?("name").should be_true
      doc.as_h.has_key?("age").should be_true
      doc.as_h.has_key?("city").should be_false
    end

    it "limit: and skip: pagination" do
      db = mem_db
      (1..5).each { |i| seed(db, "doc-#{i}", n: i.to_i64) }

      result = db.find(sel("{}"), sort: [JSON::Any.new("n")], limit: 2, skip: 1)
      result[:docs].size.should eq(2)
      result[:docs].map { |d| d["n"].as_i64 }.should eq([2_i64, 3_i64])
    end

    it "result always includes warning: string" do
      db = mem_db
      seed(db, "doc-1", type: "note")

      result = db.find(sel("{}"))
      result[:warning].should_not be_nil
      result[:warning].not_nil!.should_not be_empty
    end

    it "unknown operator raises ArgumentError" do
      db = mem_db
      seed(db, "doc-1", type: "note")

      expect_raises(ArgumentError, "Unknown operator") do
        db.find(sel(%({"type": {"$unknown": "note"}})))
      end
    end
  end
end
