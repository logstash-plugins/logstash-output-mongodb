# encoding: utf-8
require_relative "../spec_helper"

describe ::LogStash::Timestamp do
  let(:time_array) { [1918,11,11,11,0,0, "+00:00"] }
  let(:a_time) { Time.utc(*time_array) }
  let(:bson_time) { Time.utc(*time_array).to_bson }

  subject(:timestamp) { described_class.new(a_time) }

  it "responds to to_bson" do
    expect(subject).to respond_to(:to_bson)
  end

  it "to_bson returns a binary encoded timestamp which may be encoded back from bson" do
    expect(Time::from_bson(timestamp.to_bson)).to eq(Time::from_bson(bson_time))
  end

  it "bson_type returns a binary encoded 9" do
    expect(subject.bson_type).to eq(a_time.bson_type)
  end

  describe "class methods" do
    it "builds a new Timestamp from BSON" do
      expected = ::LogStash::Timestamp.new(a_time)
      decoded = ::LogStash::Timestamp.from_bson(bson_time)
      expect(decoded <=> expected).to eq(0)
    end
  end
end
