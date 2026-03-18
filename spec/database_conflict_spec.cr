require "./spec_helper"

private def mem_db : CouchDB::Database
  CouchDB::Database.new(":memory:")
end

private class Widget < CouchDB::Document
  property name : String = ""
  property qty : Int32 = 0
end

private def make_doc(id : String, **fields) : CouchDB::Document
  doc = CouchDB::Document.new
  doc.id = id
  fields.each { |k, v| doc[k.to_s] = JSON::Any.new(v) }
  doc
end

describe CouchDB::Database do
  describe "#on_conflict (put resolver)" do
    it "is not called when put succeeds without conflict" do
      db = mem_db
      called = false
      db.on_conflict { |_e, _a| called = true; nil }
      db.put(make_doc("no-conflict", v: "1"))
      called.should be_false
    end

    it "re-raises Conflict when no hook is registered" do
      db = mem_db
      db.put(make_doc("no-hook", v: "1"))
      stale = make_doc("no-hook", v: "2")
      stale.rev = "0-stale"
      expect_raises(CouchDB::Conflict) { db.put(stale) }
    end

    it "re-raises Conflict when hook returns nil" do
      db = mem_db
      db.put(make_doc("reraise", v: "1"))
      db.on_conflict { |_e, _a| nil }
      stale = make_doc("reraise", v: "2")
      stale.rev = "0-stale"
      expect_raises(CouchDB::Conflict) { db.put(stale) }
    end

    it "retries with the resolved doc and writes successfully" do
      db = mem_db
      db.put(make_doc("merge-me", v: "original"))
      db.on_conflict { |_existing, attempted| attempted }
      stale = make_doc("merge-me", v: "updated")
      stale.rev = "0-stale"
      result = db.put(stale)
      result[:ok].should be_true
      db.get("merge-me")["v"].as_s.should eq("updated")
    end

    it "hook receives the current existing doc and the attempted doc" do
      db = mem_db
      r1 = db.put(make_doc("inspect-me", v: "original"))
      seen_existing_rev = nil
      seen_attempted_v = nil
      db.on_conflict do |existing, attempted|
        seen_existing_rev = existing.rev
        seen_attempted_v = attempted["v"].as_s
        nil
      end
      stale = make_doc("inspect-me", v: "new-value")
      stale.rev = "0-stale"
      expect_raises(CouchDB::Conflict) { db.put(stale) }
      seen_existing_rev.should eq(r1[:rev])
      seen_attempted_v.should eq("new-value")
    end

    it "supports field-merging in hook" do
      db = mem_db
      db.put(make_doc("counter", count: "5"))
      db.on_conflict do |existing, attempted|
        merged = CouchDB::Document.new
        merged.id = existing.id
        merged["count"] = JSON::Any.new(
          (existing["count"].as_s.to_i + attempted["count"].as_s.to_i).to_s
        )
        merged
      end
      stale = make_doc("counter", count: "3")
      stale.rev = "0-stale"
      db.put(stale)
      db.get("counter")["count"].as_s.to_i.should eq(8)
    end

    it "propagates a custom exception raised inside the hook" do
      db = mem_db
      db.put(make_doc("custom-ex", v: "1"))
      db.on_conflict { |_e, _a| raise ArgumentError.new("my custom error") }
      stale = make_doc("custom-ex", v: "2")
      stale.rev = "0-stale"
      expect_raises(ArgumentError, "my custom error") { db.put(stale) }
    end
  end

  describe "#on_remove_conflict" do
    it "re-raises Conflict when no hook is registered" do
      db = mem_db
      db.put(make_doc("rm-no-hook"))
      expect_raises(CouchDB::Conflict) { db.remove("rm-no-hook", "0-stale") }
    end

    it "retries delete with current rev when hook returns true" do
      db = mem_db
      db.put(make_doc("rm-retry"))
      db.on_remove_conflict { |_e, _r| true }
      db.remove("rm-retry", "0-stale")
      expect_raises(CouchDB::NotFound) { db.get("rm-retry") }
    end

    it "re-raises Conflict when hook returns nil" do
      db = mem_db
      db.put(make_doc("rm-reraise"))
      db.on_remove_conflict { |_e, _r| nil }
      expect_raises(CouchDB::Conflict) { db.remove("rm-reraise", "0-stale") }
    end

    it "hook receives the existing doc and attempted rev string" do
      db = mem_db
      r1 = db.put(make_doc("rm-inspect"))
      seen_existing_rev = nil
      seen_attempted_rev = nil
      db.on_remove_conflict do |existing, attempted_rev|
        seen_existing_rev = existing.rev
        seen_attempted_rev = attempted_rev
        nil
      end
      expect_raises(CouchDB::Conflict) { db.remove("rm-inspect", "0-stale") }
      seen_existing_rev.should eq(r1[:rev])
      seen_attempted_rev.should eq("0-stale")
    end
  end

  describe "#all_docs(as:)" do
    it "returns typed rows" do
      db = mem_db
      w = Widget.new
      w.id = "w1"
      w.name = "Bolt"
      w.qty = 42
      db.put(w)
      result = db.all_docs(as: Widget)
      result[:total_rows].should eq(1_i64)
      result[:rows].first.should be_a(Widget)
      result[:rows].first.name.should eq("Bolt")
      result[:rows].first.qty.should eq(42)
    end

    it "preserves total_rows and offset" do
      db = mem_db
      3.times do |i|
        w = Widget.new
        w.id = "w#{i}"
        w.name = "item#{i}"
        db.put(w)
      end
      result = db.all_docs(as: Widget, limit: 2, skip: 1)
      result[:total_rows].should eq(3_i64)
      result[:offset].should eq(1)
      result[:rows].size.should eq(2)
    end

    it "respects startkey/endkey" do
      db = mem_db
      %w[apple banana cherry].each do |id|
        w = Widget.new
        w.id = id
        w.name = id
        db.put(w)
      end
      result = db.all_docs(as: Widget, startkey: "banana", endkey: "cherry")
      result[:rows].map(&.name).should eq(%w[banana cherry])
    end

    it "returns an empty array when no docs match" do
      db = mem_db
      result = db.all_docs(as: Widget)
      result[:rows].should be_empty
      result[:total_rows].should eq(0_i64)
    end
  end
end
