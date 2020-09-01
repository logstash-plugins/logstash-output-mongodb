# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"

describe LogStash::Outputs::Mongodb do

  let(:uri) { 'mongodb://localhost:27017' }
  let(:database) { 'logstash' }
  let(:collection) { 'logs' }

  describe "validate_config method" do

    subject! { LogStash::Outputs::Mongodb.new(config) }

    [
        {:action => "not-supported", :query_key => "qk", :query_value => "qv", :upsert => false,
         :expected_reason => "Only insert, update and replace are valid for 'action' setting."},
        {:action => "update", :query_key => "qk", :query_value => nil, :upsert => false,
         :expected_reason => "If action is update or replace, query_value must be set."},
        {:action => "update", :query_key => "qk", :query_value => "", :upsert => false,
         :expected_reason => "If action is update or replace, query_value must be set."},
        {:action => "replace", :query_key => "qk", :query_value => nil, :upsert => false,
         :expected_reason => "If action is update or replace, query_value must be set."},
        {:action => "replace", :query_key => "qk", :query_value => "", :upsert => false,
         :expected_reason => "If action is update or replace, query_value must be set."},
        {:action => "insert", :bulk_size => 1001,
         :expected_reason => "Bulk size must be lower than '1000', currently '1001'"},
    ].each do |test|

      describe "when validating config with action '#{test[:action]}' query_key '#{test[:query_key]}', query_value '#{test[:query_value]}' and upsert '#{test[:upsert]}'" do

        let(:config) {
          configuration = {
              "uri" => uri,
              "database" => database,
              "collection" => collection
          }
          unless test[:action].nil?
            configuration["action"] = test[:action]
          end
          unless test[:query_key].nil?
            configuration["query_key"] = test[:query_key]
          end
          unless test[:query_value].nil?
            configuration["query_value"] = test[:query_value]
          end
          unless test[:upsert].nil?
            configuration["upsert"] = test[:upsert]
          end
          unless test[:bulk_size].nil?
            configuration["bulk_size"] = test[:bulk_size]
          end
          return configuration
        }

        it "should raise error: #{test[:expected_reason]}" do
          expect { subject.validate_config }.to raise_error(LogStash::ConfigurationError, test[:expected_reason])
        end
      end

    end

  end
end
