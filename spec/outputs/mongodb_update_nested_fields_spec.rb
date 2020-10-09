# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri) { 'mongodb://localhost:27017' }
  let(:database) { 'logstash' }
  let(:collection) { 'logs' }
  let(:action) { 'update' }
  let(:filter) { {"_id" => 'foo' } }

  let(:config) { {
      "uri" => uri,
      "database" => database,
      "collection" => collection,
      "action" => action,
      "filter" => filter,
  } }

  describe "receive method while action is 'update'" do
    subject! { LogStash::Outputs::Mongodb.new(config) }

    let(:properties) { {
        "message" => "This is a message!",
        "rootHashField" => {
            "numFieldInHash" => 1,
            "hashFieldInHash" => {
                "numField": 2
            },
            "arrayFieldInHash" => ["one", "two", "three"]
        },
        "rootArrayField": [
            {"strFieldInArray" => "four"},
            {"strFieldInArray" => "five"},
            {"strFieldInArray" => "six"}
        ],
        "nestedArrayField": [
            {"strFieldInArray" => "four", "arrayFieldInArray" => [3, 4], "hashFieldInArray" => {"numField" => 9}},
            {"strFieldInArray" => "five", "arrayFieldInArray" => [5, 6], "hashFieldInArray" => {"numField" => 10}},
            {"strFieldInArray" => "six", "arrayFieldInArray" => [7, 8], "hashFieldInArray" => {"numField" => 11}}
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

      it "should send a document update to mongodb with dotted notation for fields in inner hashes" do
        expect(event).to receive(:timestamp).and_return(nil)
        expect(event).to receive(:to_hash).and_return(properties)
        expect(collection).to receive(:bulk_write).with(
            [{:update_one => {:filter => {"_id" => 'foo' }, :update => {"$set" => {
                "message" => "This is a message!",
                "rootHashField.numFieldInHash" => 1,
                "rootHashField.hashFieldInHash.numField" => 2,
                "rootHashField.arrayFieldInHash" => ["one", "two", "three"],
                "rootArrayField" => [
                    {"strFieldInArray" => "four"},
                    {"strFieldInArray" => "five"},
                    {"strFieldInArray" => "six"}
                ],
                "nestedArrayField" => [
                    {"strFieldInArray" => "four", "arrayFieldInArray" => [3, 4], "hashFieldInArray" => {"numField" => 9}},
                    {"strFieldInArray" => "five", "arrayFieldInArray" => [5, 6], "hashFieldInArray" => {"numField" => 10}},
                    {"strFieldInArray" => "six", "arrayFieldInArray" => [7, 8], "hashFieldInArray" => {"numField" => 11}}
                ],
            }}, :upsert => false}}]
        )
        subject.receive(event)
      end
    end

  end
end
