# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri)        { 'mongodb://localhost:27017' }
  let(:database)   { 'logstash' }
  let(:collection) { 'logs' }

  let(:config) do
    { "uri" => uri, "database" => database, "collection" => collection }
  end

  it "should register" do
    plugin = LogStash::Plugin.lookup("output", "mongodb").new(config)
    expect {plugin.register}.to_not raise_error
  end

  describe "#send" do

    subject! { LogStash::Outputs::Mongodb.new(config) }

    let(:properties) { { "message" => "This is a message!",
                         "uuid" => SecureRandom.uuid,
                         "number" => BigDecimal.new("4321.1234"),
                         "utf8" => "żółć"} }
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

    it "should send the event to the database" do
      expect(collection).to receive(:insert_one)
      subject.receive(event)
    end
  end

end
