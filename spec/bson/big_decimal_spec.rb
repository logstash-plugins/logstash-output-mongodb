# encoding: utf-8
require 'bigdecimal'
require_relative "../spec_helper"
require 'stringio'

describe ::BigDecimal do
  let(:a_number) { "4321.1234" }
  let(:bson_number) { 4321.1234.to_bson }

  subject { described_class.new(a_number) }

  it "responds to to_bson" do
    expect(subject).to respond_to(:to_bson)
  end

  it "to_bson returns a binary encoded number" do
    expect(subject.to_bson).to eq(4321.1234.to_bson)
  end

  it "bson_type returns a binary encoded 1" do
    expect(subject.bson_type).to eq(12.34.bson_type)
  end

  describe "class methods" do
    it "builds a new BigDecimal from BSON" do
      decoded = described_class.from_bson(StringIO.new(4321.1234.to_bson))
      expect(decoded).to eql(BigDecimal.new(a_number))
    end
  end
end
