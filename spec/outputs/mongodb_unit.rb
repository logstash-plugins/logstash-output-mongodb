# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require_relative "../spec_helper"

describe "mongodb unit tests" do
  let(:uri)        { "mongodb://localhost:27017" }
  let(:database)   { "logstash" }
  let(:collection) { "logs" }
  let(:action) { "insert" }

  let(:config) {{
    "uri" => uri,
    "database" => database,
    "collection" => collection,
    "action" => action
  }}

  subject! { LogStash::Outputs::Mongodb.new(config) }

  context "when calling apply_event_to_hash" do

    let (:event) { LogStash::Event.new({"message" => "hello", "positive" => 1, "negative" => -1}) }

    it "should preserve string type for values given the field reference syntax" do
      h = {"key" => "[message]"}
      applied_hash = subject.apply_event_to_hash(event, h)
      expect(applied_hash["key"]).to eql "hello"
    end

    it "should preserve positive int type for values given the field reference syntax" do
      h = {"key" => "[positive]"}
      applied_hash = subject.apply_event_to_hash(event, h)
      expect(applied_hash["key"]).to eql 1
    end

    it "should preserve negative int type for values given the field reference syntax" do
      h = {"key" => "[negative]"}
      applied_hash = subject.apply_event_to_hash(event, h)
      expect(applied_hash["key"]).to eql(-1)
    end

    it "should always interpolate strings for keys given the sprintf syntax" do
      h = {"key_%{positive}" => %{message}}
      applied_hash = subject.apply_event_to_hash(event, h)
      expect(applied_hash).to have_key("key_1")
    end
  end
end
