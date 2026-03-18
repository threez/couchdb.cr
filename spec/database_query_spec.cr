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

describe CouchDB::Database do
  describe "#query" do
    it "returns basic row shape with id, key, value" do
      db = mem_db
      seed(db, "doc-1", type: "note", count: 1_i64)

      result = db.query do |doc, emit|
        emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(doc["count"].as_i64))
      end

      result[:total_rows].should eq(1_i64)
      result[:offset].should eq(0)
      row = result[:rows].first
      row["id"].as_s.should eq("doc-1")
      row["key"].as_s.should eq("note")
      row["value"].as_i64.should eq(1_i64)
    end

    it "supports multiple emits per document" do
      db = mem_db
      seed(db, "doc-1", tags: "a")

      result = db.query do |_, emit|
        emit.call(JSON::Any.new("tag-a"), JSON::Any.new(nil))
        emit.call(JSON::Any.new("tag-b"), JSON::Any.new(nil))
      end

      result[:total_rows].should eq(2_i64)
      result[:rows].map(&.["key"].as_s).should eq(["tag-a", "tag-b"])
    end

    it "filters by exact key" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")

      result = db.query(key: JSON::Any.new("note")) do |doc, emit|
        emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(nil))
      end

      result[:rows].size.should eq(1)
      result[:rows].first["id"].as_s.should eq("doc-1")
    end

    it "filters by multiple keys (keys:)" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")
      seed(db, "doc-3", type: "event")

      result = db.query(keys: [JSON::Any.new("note"), JSON::Any.new("event")]) do |doc, emit|
        emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(nil))
      end

      result[:rows].size.should eq(2)
      result[:rows].map(&.["key"].as_s).sort!.should eq(["event", "note"])
    end

    it "filters by startkey/endkey range (inclusive)" do
      db = mem_db
      seed(db, "doc-a", label: "a")
      seed(db, "doc-b", label: "b")
      seed(db, "doc-c", label: "c")
      seed(db, "doc-d", label: "d")

      result = db.query(startkey: JSON::Any.new("b"), endkey: JSON::Any.new("c")) do |doc, emit|
        emit.call(JSON::Any.new(doc["label"].as_s), JSON::Any.new(nil))
      end

      result[:rows].map(&.["key"].as_s).should eq(["b", "c"])
    end

    it "applies limit and skip; total_rows reflects pre-skip count" do
      db = mem_db
      ("a".."e").each_with_index { |char, idx| seed(db, "doc-#{idx}", label: char) }

      result = db.query(limit: 2, skip: 1) do |doc, emit|
        emit.call(JSON::Any.new(doc["label"].as_s), JSON::Any.new(nil))
      end

      result[:total_rows].should eq(5_i64)
      result[:offset].should eq(1)
      result[:rows].size.should eq(2)
      result[:rows].first["key"].as_s.should eq("b")
    end

    it "returns rows in descending order" do
      db = mem_db
      seed(db, "doc-1", label: "a")
      seed(db, "doc-2", label: "b")
      seed(db, "doc-3", label: "c")

      result = db.query(descending: true) do |doc, emit|
        emit.call(JSON::Any.new(doc["label"].as_s), JSON::Any.new(nil))
      end

      result[:rows].map(&.["key"].as_s).should eq(["c", "b", "a"])
    end

    it "descending + startkey/endkey swaps bounds correctly" do
      db = mem_db
      ("a".."e").each_with_index { |char, idx| seed(db, "doc-#{idx}", label: char) }

      # descending from "d" down to "b"
      result = db.query(
        descending: true,
        startkey: JSON::Any.new("d"),
        endkey: JSON::Any.new("b")
      ) do |doc, emit|
        emit.call(JSON::Any.new(doc["label"].as_s), JSON::Any.new(nil))
      end

      result[:rows].map(&.["key"].as_s).should eq(["d", "c", "b"])
    end

    it "embeds full doc when include_docs: true" do
      db = mem_db
      seed(db, "doc-1", type: "note")

      result = db.query(include_docs: true) do |doc, emit|
        emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(nil))
      end

      row = result[:rows].first
      row["doc"]["_id"].as_s.should eq("doc-1")
      row["doc"]["type"].as_s.should eq("note")
    end

    it "omits doc key when include_docs: false (default)" do
      db = mem_db
      seed(db, "doc-1", type: "note")

      result = db.query do |doc, emit|
        emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(nil))
      end

      result[:rows].first.as_h.has_key?("doc").should be_false
    end

    it "_count reduce without group returns single row with key: null" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")
      seed(db, "doc-3", type: "note")

      result = db.query(reduce: "_count") do |doc, emit|
        emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(nil))
      end

      result[:rows].size.should eq(1)
      result[:rows].first["key"].raw.should be_nil
      result[:rows].first["value"].as_i64.should eq(3_i64)
    end

    it "_count with group: true returns one row per key" do
      db = mem_db
      seed(db, "doc-1", type: "note")
      seed(db, "doc-2", type: "task")
      seed(db, "doc-3", type: "note")

      result = db.query(reduce: "_count", group: true) do |doc, emit|
        emit.call(JSON::Any.new(doc["type"].as_s), JSON::Any.new(nil))
      end

      rows_by_key = result[:rows].to_h { |row| {row["key"].as_s, row["value"].as_i64} }
      rows_by_key["note"].should eq(2_i64)
      rows_by_key["task"].should eq(1_i64)
    end

    it "_sum reduces numeric values; raises ArgumentError on non-numeric" do
      db = mem_db
      seed(db, "doc-1", score: 10_i64)
      seed(db, "doc-2", score: 20_i64)
      seed(db, "doc-3", score: 30_i64)

      result = db.query(reduce: "_sum") do |doc, emit|
        emit.call(JSON::Any.new(nil), JSON::Any.new(doc["score"].as_i64))
      end

      result[:rows].first["value"].as_i64.should eq(60_i64)

      db2 = mem_db
      seed(db2, "doc-x", label: "hello")
      expect_raises(ArgumentError, "_sum requires numeric values") do
        db2.query(reduce: "_sum") do |doc, emit|
          emit.call(JSON::Any.new(nil), JSON::Any.new(doc["label"].as_s))
        end
      end
    end

    it "_stats reduce returns sum/count/min/max/sumsq" do
      db = mem_db
      seed(db, "doc-1", score: 2_i64)
      seed(db, "doc-2", score: 4_i64)
      seed(db, "doc-3", score: 6_i64)

      result = db.query(reduce: "_stats") do |doc, emit|
        emit.call(JSON::Any.new(nil), JSON::Any.new(doc["score"].as_i64))
      end

      stats = result[:rows].first["value"]
      stats["sum"].as_f.should eq(12.0)
      stats["count"].as_i64.should eq(3_i64)
      stats["min"].as_f.should eq(2.0)
      stats["max"].as_f.should eq(6.0)
      stats["sumsq"].as_f.should eq(56.0)
    end

    it "group_level truncates array key to first N elements" do
      db = mem_db
      seed(db, "doc-1", year: 2024_i64, month: 1_i64)
      seed(db, "doc-2", year: 2024_i64, month: 2_i64)
      seed(db, "doc-3", year: 2025_i64, month: 1_i64)

      result = db.query(reduce: "_count", group_level: 1) do |doc, emit|
        key = JSON::Any.new([JSON::Any.new(doc["year"].as_i64), JSON::Any.new(doc["month"].as_i64)])
        emit.call(key, JSON::Any.new(nil))
      end

      rows_by_year = result[:rows].to_h { |row| {row["key"].as_a.first.as_i64, row["value"].as_i64} }
      rows_by_year[2024_i64].should eq(2_i64)
      rows_by_year[2025_i64].should eq(1_i64)
    end

    it "raises ArgumentError for unknown reduce function; empty DB returns empty rows" do
      db = mem_db

      result = db.query do |_, emit|
        emit.call(JSON::Any.new(nil), JSON::Any.new(nil))
      end
      result[:rows].should be_empty
      result[:total_rows].should eq(0_i64)

      db2 = mem_db
      seed(db2, "doc-1", type: "note")
      expect_raises(ArgumentError, "Unknown reduce function") do
        db2.query(reduce: "_custom") do |_, emit|
          emit.call(JSON::Any.new(nil), JSON::Any.new(nil))
        end
      end
    end
  end
end
