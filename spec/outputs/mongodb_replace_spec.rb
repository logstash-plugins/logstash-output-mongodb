# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri) { 'mongodb://localhost:27017' }
  let(:database) { 'logstash' }
  let(:collection) { 'logs' }
  let(:action) { 'replace' }

  let(:config) { {
      "uri" => uri,
      "database" => database,
      "collection" => collection,
      "action" => action
  } }

  describe "receive method while action is 'replace'" do
    subject! { LogStash::Outputs::Mongodb.new(config) }

    let(:properties) { {
        "message" => "This is a message!",
        "uuid" => "00000000-0000-0000-0000-000000000000",
        "number" => BigDecimal.new("4321.1234"),
        "utf8" => "żółć"
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

    [
      {:filter => {"_id" => "[uuid]"}, :upsert => false,
       :expected => {:filter => {"_id" => "00000000-0000-0000-0000-000000000000"}, :upsert => false}
      },
      {:filter => {"%{utf8}" => "[message]"}, :upsert => nil,
       :expected => {:filter => {"żółć" => "This is a message!"}, :upsert => false}
      },
      {:filter => {"%{utf8}" => "[message]"}, :upsert => true,
       :expected => {:filter => {"żółć" => "This is a message!"}, :upsert => true}
      },
    ].each do |test|

      describe "when processing an event with :filter => '#{test[:filter]}' and :upsert => '#{test[:upsert]}'" do

        let(:config) {
          configuration = {
              "uri" => uri,
              "database" => database,
              "collection" => collection,
              "action" => action
          }
          unless test[:filter].nil?
            configuration["filter"] = test[:filter]
          end
          unless test[:upsert].nil?
            configuration["upsert"] = test[:upsert]
          end
          return configuration
        }

        expected = test[:expected]
        it "should send that document as a replace to mongodb with :filter => '#{expected[:filter]}' and upsert => '#{expected[:upsert]}'" do
          expect(event).to receive(:timestamp).and_return(nil)
          expect(event).to receive(:to_hash).and_return(properties)
          expect(collection).to receive(:bulk_write).with(
              [{:replace_one => {:filter => expected[:filter], :replacement => properties, :upsert => expected[:upsert]}}]
          )
          subject.receive(event)
        end
      end

    end

  end
end
