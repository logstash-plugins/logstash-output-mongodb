# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri)        { 'mongodb://localhost:27017' }
  let(:database)   { 'logstash' }
  let(:collection) { 'logs' }
  let(:action) { 'insert' }

  let(:config) {{
    "uri" => uri,
    "database" => database,
    "collection" => collection,
    "action" => action
  }}

  it "should register and close" do
    plugin = LogStash::Plugin.lookup("output", "mongodb").new(config)
    expect {plugin.register}.to_not raise_error
    plugin.close
  end

  describe "receive method while action is 'insert'" do
    subject! { LogStash::Outputs::Mongodb.new(config) }

    let(:event)      { LogStash::Event.new(properties) }
    let(:connection) { double("connection") }
    let(:client)     { double("client") }
    let(:collection) { double("collection") }

    before(:each) do
      allow(Mongo::Client).to receive(:new).and_return(connection)
      allow(connection).to receive(:use).and_return(client)
      allow(client).to receive(:[]).and_return(collection)
      allow(collection).to receive(:bulk_write)
      subject.register
    end

    after(:each) do
      subject.close
    end

    describe "when processing an event" do
      let(:properties) {{
        "message" => "This is a message!",
        "uuid" => "00000000-0000-0000-0000-000000000000",
        "number" => BigDecimal.new("4321.1234"),
        "utf8" => "żółć"
      }}

      it "should send the event to the database" do
        expect(collection).to receive(:bulk_write)
        subject.receive(event)
      end
    end

    describe "when processing an event without @timestamp set" do
      let(:properties) { { "message" => "foo" } }

      it "should send a document without @timestamp field to mongodb" do
        expect(event).to receive(:timestamp).and_return(nil)
        expect(event).to receive(:to_hash).and_return(properties)
        expect(collection).to receive(:bulk_write).with(
            [ {:insert_one => properties} ]
        )
        subject.receive(event)
      end
    end

    describe "when generateId is set" do
      let(:properties) { { "message" => "foo" } }
      let(:config) {{
          "uri" => uri,
          "database" => database,
          "collection" => collection,
          "generateId" => true,
          "action" => "insert"
      }}

      it "should send a document containing a BSON::ObjectId as _id to mongodb" do
        expect(BSON::ObjectId).to receive(:new).and_return("BSON::ObjectId")
        expect(event).to receive(:timestamp).and_return(nil)
        expect(event).to receive(:to_hash).and_return(properties)
        expect(collection).to receive(:bulk_write).with(
            [ {:insert_one => properties.merge("_id" => "BSON::ObjectId")} ]
        )
        subject.receive(event)
      end
    end
  end
end
