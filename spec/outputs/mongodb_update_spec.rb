# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri) { 'mongodb://localhost:27017' }
  let(:database) { 'logstash' }
  let(:collection) { 'logs' }
  let(:action) { 'update' }

  let(:config) { {
      "uri" => uri,
      "database" => database,
      "collection" => collection,
      "action" => action
  } }

  describe "receive method while action is 'update'" do
    subject! { LogStash::Outputs::Mongodb.new(config) }

    let(:properties) { {
        "message" => "This is a message!",
        "uuid" => SecureRandom.uuid,
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
        {:query_key => nil, :query_value => "qv", :upsert => false,
         :expected => {:query_key => "_id", :query_value => "qv", :upsert => false}
        },
        {:query_key => "qk", :query_value => "qv", :upsert => false,
         :expected => {:query_key => "qk", :query_value => "qv", :upsert => false}
        },
        {:query_key => "qk", :query_value => "qv", :upsert => nil,
         :expected => {:query_key => "qk", :query_value => "qv", :upsert => false}
        },
        {:query_key => nil, :query_value => "qv", :upsert => true,
         :expected => {:query_key => "_id", :query_value => "qv", :upsert => true}
        },
        {:query_key => "qk", :query_value => "qv", :upsert => true,
         :expected => {:query_key => "qk", :query_value => "qv", :upsert => true}
        },
    ].each do |test|

      describe "when processing an event with query_key set to '#{test[:query_key]}', query_value set to '#{test[:query_value]}' and upsert set to '#{test[:upsert]}'" do

        let(:config) {
          configuration = {
              "uri" => uri,
              "database" => database,
              "collection" => collection,
              "action" => action
          }
          unless test[:query_key].nil?
            configuration["query_key"] = test[:query_key]
          end
          unless test[:query_value].nil?
            configuration["query_value"] = test[:query_value]
          end
          unless test[:upsert].nil?
            configuration["upsert"] = test[:upsert]
          end
          return configuration
        }

        expected = test[:expected]
        it "should send that document as an update to mongodb with query_key '#{expected[:query_key]}', query_value '#{expected[:query_value]}' and upsert '#{expected[:upsert]}'" do
          expect(event).to receive(:timestamp).and_return(nil)
          expect(event).to receive(:to_hash).and_return(properties)
          expect(collection).to receive(:bulk_write).with(
              [{:update_one => {:filter => {expected[:query_key] => expected[:query_value]}, :update => {"$set" => properties}, :upsert => expected[:upsert]}}]
          )
          subject.receive(event)
        end
      end

    end

  end
end
