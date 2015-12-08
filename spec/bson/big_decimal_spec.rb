# encoding: utf-8
require 'bigdecimal'
require_relative "../spec_helper"
require 'stringio'

describe ::BigDecimal do
  let(:a_number) { "4321.1234" }
  let(:bson_number) { "Tt$\x97\x1F\xE1\xB0@".force_encoding(BSON::BINARY) }

  subject { described_class.new(a_number) }

  it "responds to to_bson" do
    expect(subject).to respond_to(:to_bson)
  end

  it "to_bson returns a binary encoded number" do
    expect(subject.to_bson).to eq(bson_number)
  end

  it "bson_type returns a binary encoded 1" do
    expect(subject.bson_type).to eq("\x01".force_encoding(BSON::BINARY))
  end

  describe "class methods" do
    it "builds a new BigDecimal from BSON" do
      decoded = described_class.from_bson(StringIO.new(bson_number))
      expect(decoded).to eq(subject)
      expect(decoded).to be_a(BigDecimal)
    end
  end
end
