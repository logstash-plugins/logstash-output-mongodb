# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri) { 'mongodb://localhost:27017' }
  let(:database) { 'logstash' }
  let(:collection) { 'logs' }

  describe "when validating config" do

    subject! { LogStash::Outputs::Mongodb.new(config) }

    [
        {:update_expressions => {"invalid-expression" => "foo"},
         :expected_reason => "The :update_expressions option contains 'invalid-expression', which is not an Update expression."},
        {:action => "insert", :bulk_size => 1001,
         :expected_reason => "Bulk size must be lower than '1000', currently '1001'"},
    ].each do |test|

      describe "with :bulk_size => '#{test[:bulk_size]}', :upsert => '#{test[:upsert]}' and :update_expressions => '#{test[:update_expressions]}'" do

        let(:config) do
          configuration = {
              "uri" => uri,
              "database" => database,
              "collection" => collection,
              "filter" => {"_id" => "123"},
              "action" => "update"
          }
          unless test[:bulk_size].nil?
            configuration["bulk_size"] = test[:bulk_size]
          end
          unless test[:update_expressions].nil?
            configuration["update_expressions"] = test[:update_expressions]
          end
          return configuration
        end

        it "should raise error: '#{test[:expected_reason]}'" do
          expect { subject.validate_config }.to raise_error(LogStash::ConfigurationError, test[:expected_reason])
        end
      end
    end
  end

  describe "when validating action" do

    subject! { LogStash::Outputs::Mongodb.new(config) }

    [
        {:action => "unsupported", :filter => {"_id" => "123"}, :upsert => false,
         :expected_reason => "Only insert, update and replace are supported Mongo actions, got 'unsupported'."},
        {:action => "delete", :filter => {"_id" => "123"}, :upsert => false,
         :expected_reason => "Only insert, update and replace are supported Mongo actions, got 'delete'."},
        {:action => "update", :filter => {}, :upsert => false,
         :expected_reason => "If action is update or replace, filter must be set."},
        {:action => "%{myaction}", :filter => {}, :upsert => false,
         :expected_reason => "If action is update or replace, filter must be set."},
        {:action => "%{[myactionnested][foo]}", :filter => {}, :upsert => false,
         :expected_reason => "If action is update or replace, filter must be set."},
        {:action => "update", :filter => nil, :upsert => false,
         :expected_reason => "If action is update or replace, filter must be set."},
        {:action => "insert", :update_expressions => {"$inc" => {"quantity" => 1}},
         :expected_reason => "The :update_expressions only makes sense if the action is an update."},
        {:action => "replace",  :filter => {"_id" => "123"}, :update_expressions => {"$inc" => {"quantity" => 1}},
         :expected_reason => "The :update_expressions only makes sense if the action is an update."},
    ].each do |test|

      describe "with :action => '#{test[:action]}', :filter => '#{test[:filter]}', :upsert => '#{test[:upsert]}' and :update_expressions => '#{test[:update_expressions]}'" do

        let(:event) { LogStash::Event.new("myaction" => "update", "myactionnested" => {"foo" => "replace"})}

        let(:config) do
          configuration = {
              "uri" => uri,
              "database" => database,
              "collection" => collection
          }
          unless test[:action].nil?
            configuration["action"] = test[:action]
          end
          unless test[:filter].nil?
            configuration["filter"] = test[:filter]
          end
          unless test[:upsert].nil?
            configuration["upsert"] = test[:upsert]
          end
          unless test[:update_expressions].nil?
            configuration["update_expressions"] = test[:update_expressions]
          end
          return configuration
        end

        it "should raise error: '#{test[:expected_reason]}'" do
          expect { subject.receive(event) }.to raise_error(LogStash::ConfigurationError, test[:expected_reason])
        end
      end
    end
  end
end
