# encoding: utf-8
require_relative "../spec_helper"
require 'stringio'

describe ::LogStash::Timestamp do
  let(:a_time) { Time.new(1918,11,11,11,0,0) }
  let(:bson_time) { "\x80{y@\x88\xFE\xFF\xFF".force_encoding(BSON::BINARY) }

  subject { described_class.new(a_time) }

  it "responds to to_bson" do
    expect(subject).to respond_to(:to_bson)
  end

  it "to_bson returns a binary encoded timestamp" do
    expect(subject.to_bson).to eq(bson_time)
  end

  it "bson_type returns a binary encoded 9" do
    expect(subject.bson_type).to eq("\x09".force_encoding(BSON::BINARY))
  end

  describe "class methods" do
    it "builds a new Timestamp from BSON" do
      decoded = described_class.from_bson(StringIO.new(bson_time))
      expect(decoded).to eq(subject)
      expect(decoded).to be_a(::LogStash::Timestamp)
    end
  end
end
