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

  # The method used to write processed events to MongoDB.
  # Possible values are `insert`, `update` and `replace`.
  config :action, :validate => :string, :required => true
  # The key of the query to find the document to update or replace.
  config :query_key, :validate => :string, :required => false, :default => "_id"
  # The value of the query to find the document to update or replace. This can be dynamic using the `%{foo}` syntax.
  config :query_value, :validate => :string, :required => false
  # If true, a new document is created if no document exists in DB with given `document_id`.
  # Only applies if action is `update` or `replace`.
  config :upsert, :validate => :boolean, :required => false, :default => false

  # Mutex used to synchronize access to 'documents'
  @@mutex = Mutex.new

  def register

    validate_config

    Mongo::Logger.logger = @logger
    conn = Mongo::Client.new(@uri)
    @db = conn.use(@database)

    @closed = Concurrent::AtomicBoolean.new(false)
    @documents = {}

    @bulk_thread = Thread.new(@bulk_interval) do |bulk_interval|
      while @closed.false? do
        sleep(bulk_interval)

        @@mutex.synchronize do
          @documents.each do |collection, values|
            if values.length > 0
              write_to_mongodb(collection, values)
              @documents.delete(collection)
            end
          end
        end
      end
    end
  end

  def validate_config
    if @bulk_size > 1000
      raise LogStash::ConfigurationError, "Bulk size must be lower than '1000', currently '#{@bulk_size}'"
    end
    if @action != "insert" && @action != "update" && @action != "replace"
      raise LogStash::ConfigurationError, "Only insert, update and replace are valid for 'action' setting."
    end
    if (@action == "update" || @action == "replace") && (@query_value.nil? || @query_value.empty?)
      raise LogStash::ConfigurationError, "If action is update or replace, query_value must be set."
    end
  end

  def receive(event)
    begin
      # Our timestamp object now has a to_bson method, using it here
      # {}.merge(other) so we don't taint the event hash innards
      document = {}.merge(event.to_hash)

      if !@isodate
        timestamp = event.timestamp
        if timestamp
          # not using timestamp.to_bson
          document["@timestamp"] = timestamp.to_json
        else
          @logger.warn("Cannot set MongoDB document `@timestamp` field because it does not exist in the event", :event => event)
        end
      end

      if @generateId
        document["_id"] = BSON::ObjectId.new
      end

      collection = event.sprintf(@collection)
      if @action == "update" or @action == "replace"
        document["metadata_mongodb_output_query_value"] = event.sprintf(@query_value)
      end
      if @bulk
        @@mutex.synchronize do
          if(!@documents[collection])
            @documents[collection] = []
          end
          @documents[collection].push(document)

          if(@documents[collection].length >= @bulk_size)
            write_to_mongodb(collection, @documents[collection])
            @documents.delete(collection)
          end
        end
      else
        write_to_mongodb(collection, [document])
      end
    rescue => e
      if e.message =~ /^E11000/
        # On a duplicate key error, skip the insert.
        # We could check if the duplicate key err is the _id key
        # and generate a new primary key.
        # If the duplicate key error is on another field, we have no way
        # to fix the issue.
        @logger.warn("Skipping insert because of a duplicate key error", :event => event, :exception => e)
      else
        @logger.warn("Failed to send event to MongoDB, retrying in #{@retry_delay.to_s} seconds", :event => event, :exception => e)
        sleep(@retry_delay)
        retry
      end
    end
  end

  def write_to_mongodb(collection, documents)
    ops = get_write_ops(documents)
    @db[collection].bulk_write(ops)
  end

  def get_write_ops(documents)
    ops = []
    documents.each do |doc|
      replaced_query_value = doc["metadata_mongodb_output_query_value"]
      doc.delete("metadata_mongodb_output_query_value")
      if @action == "insert"
        ops << {:insert_one => doc}
      elsif @action == "update"
        ops << {:update_one => {:filter => {@query_key => replaced_query_value}, :update => {'$set' => to_dotted_hash(doc)}, :upsert => @upsert}}
      elsif @action == "replace"
        ops << {:replace_one => {:filter => {@query_key => replaced_query_value}, :replacement => doc, :upsert => @upsert}}
      end
    end
    ops
  end

  def to_dotted_hash(hash, recursive_key = "")
    hash.each_with_object({}) do |(k, v), ret|
      key = recursive_key + k.to_s
      if v.is_a? Array
        v.each_with_index do |arrV, i|
          arrKey = key + "." + i.to_s
          if arrV.is_a? Hash
            ret.merge! to_dotted_hash(arrV, arrKey + ".")
          else
            ret[arrKey] = arrV
          end
        end
      elsif v.is_a? Hash
        ret.merge! to_dotted_hash(v, key + ".")
      else
        ret[key] = v
      end
    end
  end

  def close
    @closed.make_true
    @bulk_thread.wakeup
    @bulk_thread.join
  end
end
