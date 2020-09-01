# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri) { 'mongodb://localhost:27017' }
  let(:database) { 'logstash' }
  let(:collection) { 'logs' }
  let(:action) { 'update' }
  let(:query_value) { 'qv' }

  let(:config) { {
      "uri" => uri,
      "database" => database,
      "collection" => collection,
      "action" => action,
      "query_value" => query_value
  } }

  describe "receive method while action is 'update'" do
    subject! { LogStash::Outputs::Mongodb.new(config) }

    let(:properties) { {
        "message" => "This is a message!",
        "hashField" => {
            "numField" => 1,
            "hashField" => {
                "numField": 2
            },
            "arrayField" => ["one", "two", "three"]
        },
        "arrayField": [
            {"strField" => "four"},
            {"strField" => "five"},
            {"strField" => "six"},
            "numField" => 3
        ]
    } }
    let(:event) { LogStash::Event.new(properties) }
    let(:connection) { double("connection") }
    let(:client) { double("client") }
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

    describe "when processing an event with nested hash" do

      it "should send a document update to mongodb with dotted notation" do
        expect(event).to receive(:timestamp).and_return(nil)
        expect(event).to receive(:to_hash).and_return(properties)
        expect(collection).to receive(:bulk_write).with(
            [{:update_one => {:filter => {"_id" => query_value}, :update => {"$set" => {
                "message" => "This is a message!",
                "hashField.numField" => 1,
                "hashField.hashField.numField" => 2,
                "hashField.arrayField.0" => "one",
                "hashField.arrayField.1" => "two",
                "hashField.arrayField.2" => "three",
                "arrayField.0.strField" => "four",
                "arrayField.1.strField" => "five",
                "arrayField.2.strField" => "six",
                "arrayField.3.numField" => 3,
            }}, :upsert => false}}]
        )
        subject.receive(event)
      end
    end

  end
end
