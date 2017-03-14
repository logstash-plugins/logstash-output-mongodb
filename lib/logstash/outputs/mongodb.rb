# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "mongo"
require_relative "bson/big_decimal"
require_relative "bson/logstash_timestamp"

# This output writes events to MongoDB.
class LogStash::Outputs::Mongodb < LogStash::Outputs::Base

  config_name "mongodb"

  # A MongoDB URI to connect to.
  # See http://docs.mongodb.org/manual/reference/connection-string/.
  config :uri, :validate => :string, :required => true

  # The database to use.
  config :database, :validate => :string, :required => true

  # The collection to use. This value can use `%{foo}` values to dynamically
  # select a collection based on data in the event.
  config :collection, :validate => :string, :required => true

  # If true, store the @timestamp field in MongoDB as an ISODate type instead
  # of an ISO8601 string.  For more information about this, see
  # http://www.mongodb.org/display/DOCS/Dates.
  config :isodate, :validate => :boolean, :default => false

  # The number of seconds to wait after failure before retrying.
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # If true, an "_id" field will be added to the document before insertion.
  # The "_id" field will use the timestamp of the event and overwrite an existing
  # "_id" field in the event.
  config :generateId, :validate => :boolean, :default => false

  public
  def register
    Mongo::Logger.logger = @logger
    conn = Mongo::Client.new(@uri)
    @db = conn
  end # def register

  def receive(event)
    begin
      # Our timestamp object now has a to_bson method, using it here
      # {}.merge(other) so we don't taint the event hash innards
      document = {}.merge(event.to_hash)
      if !@isodate
        # not using timestamp.to_bson
        document["@timestamp"] = event.timestamp.to_json
      end
      if @generateId
        document["_id"] = BSON::ObjectId.new(nil, event.timestamp)
      end
      document["social_monitor_sources"].each do |e|
        @db.use(e["company_uid"])[event.sprintf(e["monitor_uid"])].insert_one(document)
      end
    rescue => e
      p event.to_json
      @logger.warn("Failed to send event to MongoDB", :event => event, :exception => e,
                   :backtrace => e.backtrace)
    end
  end # def receive
end # class LogStash::Outputs::Mongodb
