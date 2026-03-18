require "./spec_helper"

describe CouchDB do
  it "exports VERSION" do
    CouchDB::VERSION.should eq("0.1.0")
  end
end
