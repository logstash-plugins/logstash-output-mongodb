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


  # Bulk insert flag, set to true to allow bulk insertion, else it will insert events one by one.
  config :bulk, :validate => :boolean, :default => false
  # Bulk interval, Used to insert events periodically if the "bulk" flag is activated.
  config :bulk_interval, :validate => :number, :default => 2
  # Bulk events number, if the number of events to insert into a collection raise that limit, it will be bulk inserted
  # whatever the bulk interval value (mongodb hard limit is 1000).
  config :bulk_size, :validate => :number, :default => 900, :maximum => 999, :min => 2

  # Mutex used to synchronize access to 'documents'
  @@mutex = Mutex.new

  public
  def register
    Mongo::Logger.logger = @logger
    conn = Mongo::Client.new(@uri)
    @db = conn.use(@database)

    if @bulk_size > 1000
      raise LogStash::ConfigurationError, "Bulk size must be lower than '1000', currently '#{@bulk_size}'"
    end
    @documents = {}
    Thread.new do
      loop do
        sleep @bulk_interval
        @@mutex.synchronize do
          @documents.each do |collection, values|
            if values.length > 0
              @db[collection].insert_many(values)
              @documents.delete(collection)
            end
          end
        end
      end
    end
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
      if @bulk
        @@mutex.synchronize do
          collection = event.sprintf(@collection)
          if(!@documents[collection])
            @documents[collection] = []
          end
          @documents[collection].push(document)

          if(@documents[collection].length >= @bulk_size)
            @db[collection].insert_many(@documents[collection])
            @documents.delete(collection)
          end
        end
      else
        @db[event.sprintf(@collection)].insert_one(document)
      end

    rescue => e
      @logger.warn("Failed to send event to MongoDB", :event => event, :exception => e,
                   :backtrace => e.backtrace)
      if e.message =~ /^E11000/
          # On a duplicate key error, skip the insert.
          # We could check if the duplicate key err is the _id key
          # and generate a new primary key.
          # If the duplicate key error is on another field, we have no way
          # to fix the issue.
      else
        sleep @retry_delay
        retry
      end
    end
  end # def receive
end # class LogStash::Outputs::Mongodb
