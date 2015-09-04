# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/mongodb"

RSpec::Matchers.define :have_received do |event|
  match do |subject|
    client     = subject.instance_variable_get("@db")
    collection = subject.instance_variable_get("@collection")
    client["#{collection}"].find("@timestamp" => event["@timestamp"].to_json).count > 0
  end
end
