# encoding: utf-8
require_relative "../spec_helper"

describe LogStash::Outputs::Mongodb, :integration => true do

  let(:uri)        { 'mongodb://localhost:27017' }
  let(:database)   { 'logstash' }
  let(:collection) { 'logs' }

  let(:config) do
    { "uri" => uri, "database" => database, "collection" => collection }
  end

  describe "#send" do

    subject { LogStash::Outputs::Mongodb.new(config) }

    let(:properties) { { "message" => "This is a message!"} }
    let(:event)      { LogStash::Event.new(properties) }

    before(:each) do
      subject.register
    end

    it "should send the event to the database" do
      subject.receive(event)
      expect(subject).to have_received(event)
    end
  end
end
