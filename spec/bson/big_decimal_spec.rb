# encoding: utf-8
require 'bigdecimal'
require_relative "../spec_helper"

describe ::BigDecimal do
  let(:a_number) { "4321.1234" }
  let(:bson_number) { 4321.1234.to_bson }

  subject { described_class.new(a_number) }

  it "responds to to_bson" do
    expect(subject).to respond_to(:to_bson)
  end

  it "to_bson returns a binary encoded number  which can be encoded back from bson" do
    expect(BigDecimal::from_bson(subject.to_bson)).to eq(BigDecimal::from_bson(4321.1234.to_bson))
  end

  it "bson_type returns a binary encoded 1" do
    expect(subject.bson_type).to eq(12.34.bson_type)
  end

  describe "class methods" do
    it "builds a new BigDecimal from BSON" do
      decoded = described_class.from_bson(4321.1234.to_bson)
      expect(decoded).to eql(BigDecimal(a_number))
    end
  end
end
