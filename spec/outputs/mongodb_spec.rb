# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri)        { 'mongodb://localhost:27017' }
  let(:database)   { 'logstash' }
  let(:collection) { 'logs' }

  let(:config) {{
    "uri" => uri,
    "database" => database,
    "collection" => collection
  }}

  it "should register and close" do
    plugin = LogStash::Plugin.lookup("output", "mongodb").new(config)
    expect {plugin.register}.to_not raise_error
    plugin.close
  end

  describe "receive" do
    describe "insert to mongodb" do
      subject! { LogStash::Outputs::Mongodb.new(config) }

      let(:event)      { LogStash::Event.new(properties) }
      let(:connection) { double("connection") }
      let(:client)     { double("client") }
      let(:collection) { double("collection") }

      before(:each) do
        allow(Mongo::Client).to receive(:new).and_return(connection)
        allow(connection).to receive(:use).and_return(client)
        allow(client).to receive(:[]).and_return(collection)
        allow(collection).to receive(:insert_one)
        subject.register
      end

      after(:each) do
        subject.close
      end

      describe "#send" do
        let(:properties) {{
          "message" => "This is a message!",
          "uuid" => SecureRandom.uuid,
          "number" => BigDecimal.new("4321.1234"),
          "utf8" => "żółć"
        }}

        it "should send the event to the database" do
          expect(collection).to receive(:insert_one)
          subject.receive(event)
        end
      end

      describe "no event @timestamp" do
        let(:properties) { { "message" => "foo" } }

        it "should not contain a @timestamp field in the mongo document" do
          expect(event).to receive(:timestamp).and_return(nil)
          expect(event).to receive(:to_hash).and_return(properties)
          expect(collection).to receive(:insert_one).with(properties)
          subject.receive(event)
        end
      end

      describe "generateId" do
        let(:properties) { { "message" => "foo" } }
        let(:config) {{
            "uri" => uri,
            "database" => database,
            "collection" => collection,
            "generateId" => true
        }}

        it "should contain a BSON::ObjectId as _id" do
          expect(BSON::ObjectId).to receive(:new).and_return("BSON::ObjectId")
          expect(event).to receive(:timestamp).and_return(nil)
          expect(event).to receive(:to_hash).and_return(properties)
          expect(collection).to receive(:insert_one).with(properties.merge("_id" => "BSON::ObjectId"))
          subject.receive(event)
        end
      end
    end

    describe "upsert/insert to mondodb" do
      let(:config) {{
        "uri" => uri,
        "database" => database,
        "collection" => collection,
        "upsert" => true
      }}

      subject! { LogStash::Outputs::Mongodb.new(config) }

      let(:event)      { LogStash::Event.new(properties) }
      let(:connection) { double("connection") }
      let(:client)     { double("client") }
      let(:collection) { double("collection") }

      before(:each) do
        allow(Mongo::Client).to receive(:new).and_return(connection)
        allow(connection).to receive(:use).and_return(client)
        allow(client).to receive(:[]).and_return(collection)
        allow(collection).to receive(:insert_one)
        allow(collection).to receive(:replace_one)
        subject.register
      end

      after(:each) do
        subject.close
      end

      describe "#send with _id" do
        let(:properties) {{
          "_id" => "esisting_id",
          "message" => "This is a message!",
          "uuid" => SecureRandom.uuid,
          "number" => BigDecimal.new("4321.1234"),
          "utf8" => "żółć"
        }}

        it "should send the event to the database using replace_one" do
          expect(collection).to receive(:replace_one)
          subject.receive(event)
        end
      end

      describe "#send without _id" do
        let(:properties) {{
          "message" => "This is a message!",
          "uuid" => SecureRandom.uuid,
          "number" => BigDecimal.new("4321.1234"),
          "utf8" => "żółć"
        }}

        it "should send the event to the database using insert_one" do
          expect(collection).to receive(:insert_one)
          subject.receive(event)
        end
      end
    end
  end
end
