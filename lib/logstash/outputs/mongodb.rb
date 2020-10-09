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

  # The Mongo DB action to perform. Valid actions are:
  #
  # - insert: inserts a document, fails if a document the document already exists.
  # - update: updates a document given a `filter`. You can also upsert a document, see the `upsert` option.
  # - delete: *Not Supported* at the moment
  #
  # A sprintf-able string is allowed to change the action based on the content
  # of the event. The value `%{[foo]}` would use the foo field for the action.
  #
  # For more details on actions, check out the https://docs.mongodb.com/ruby-driver/v2.6/tutorials/ruby-driver-bulk-operations[Mongo Ruby Driver documentation]
  config :action, :validate => :string, :default => "insert"

  # The :filter clause for an update or replace.
  #
  # A sprintf-able string is allowed for keys: the value `my_%{[foo]}` would
  # use the foo field instead *always coerced to a string*.
  #
  # Hovewever, the
  # https://www.elastic.co/guide/en/logstash/current/field-references-deepdive.html[Field
  # Reference Syntax] is required for values - these preserve type (integer,
  # float, ...).
  config :filter, :validate => :hash, :required => false, :default => {}

  # The hash in :update_expressions will be used *instead* of the default
  # '$set'. This option is useful for using alternative operators like '$inc'.
  #
  # A sprintf-able string is allowed for keys: the value `my_%{[foo]}` would
  # use the foo field instead *always coerced to a string*.
  #
  # Hovewever, the
  # https://www.elastic.co/guide/en/logstash/current/field-references-deepdive.html[Field
  # Reference Syntax] is required for values - these preserve type (integer,
  # float, ...).
  #
  # Keys must start with `$`, see the https://docs.mongodb.com/manual/reference/operator/update/#id1[Mongo DB Update Operators] for reference.
  #
  # Note that pipeline support (Mongo >= 4.2) is not there yet.
  config :update_expressions, :validate => :hash, :required => false, :default => nil

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
    if !@update_expressions.nil?
      @update_expressions.keys.each { |k|
        if !is_update_operator(k)
          raise LogStash::ConfigurationError, "The :update_expressions option contains '#{k}', which is not an Update expression."
          break
        end
      }
    end
  end

  def validate_action(action, filter, update_expressions)
    if action != "insert" && action != "update" && action != "replace"
      raise LogStash::ConfigurationError, "Only insert, update and replace are supported Mongo actions, got '#{action}'."
    end
    if (action == "update" || action == "replace") && (filter.nil? || filter.empty?)
      raise LogStash::ConfigurationError, "If action is update or replace, filter must be set."
    end
    if action != "update" && !(update_expressions.nil? || update_expressions.empty?)
      raise LogStash::ConfigurationError, "The :update_expressions only makes sense if the action is an update."
    end
  end

  def receive(event)
    action = event.sprintf(@action)

    validate_action(action, @filter, @update_expressions)

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
      if action == "update" or action == "replace"
        document["metadata_mongodb_output_filter"] = apply_event_to_hash(event, @filter)
      end

      if action == "update" and !(@update_expressions.nil? || @update_expressions.empty?)
        # we only expand the values cause keys are update expressions
        expressions_hash = {}
        @update_expressions.each do |k, v|
          expressions_hash[k] = apply_event_to_hash(event, v)
        end
        document["metadata_mongodb_output_update_expressions"] = expressions_hash
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
        result = write_to_mongodb(collection, [document])
        @logger.debug("Bulk write result", :result => result)
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
        @logger.warn("Failed to send event to MongoDB retrying in #{@retry_delay.to_s} seconds", :result=> e.result, :message => e.message)
        sleep(@retry_delay)
        retry
      end
    end
  end

  def write_to_mongodb(collection, documents)
    ops = get_write_ops(documents)
    @logger.debug("Sending", :ops => ops)
    @db[collection].bulk_write(ops)
  end

  def get_write_ops(documents)
    ops = []
    documents.each do |doc|
      filter = doc["metadata_mongodb_output_filter"]
      doc.delete("metadata_mongodb_output_filter")

      update_expressions = doc["metadata_mongodb_output_update_expressions"]
      doc.delete("metadata_mongodb_output_update_expressions")

      # TODO: support multiple expressions as pipeline for Mongo >= 4.2
      update = if !update_expressions.nil?
                 update_expressions
               else
                 {'$set' => to_dotted_hash(doc)}
               end

      if action == "insert"
        ops << {:insert_one => doc}
      elsif action == "update"
        ops << {:update_one => {:filter => filter, :update => update, :upsert => @upsert}}
      elsif action == "replace"
        ops << {:replace_one => {:filter => filter, :replacement => doc, :upsert => @upsert}}
      end
    end
    ops
  end

  def is_update_operator(string)
    string.start_with?("$")
  end

  # Apply the event to the input hash keys and values.
  #
  # This function is recursive.
  #
  # It uses event.sprintf for keys but event.get for values because it looks
  # like event.sprintf always returns a string and we don't want to always
  # coerce.
  #
  # See  https://github.com/elastic/logstash/issues/5114
  def apply_event_to_hash(event, hash)
    hash.clone.each_with_object({}) do |(k, v), ret|
      if v.is_a? Hash
        ret[event.sprintf(k)] = apply_event_to_hash(event, v)
      else
        event_value = event.get(v)
        ret[event.sprintf(k)] = event_value.nil? ? v : event_value
      end
    end
  end

  def to_dotted_hash(hash, recursive_key = "")
    hash.each_with_object({}) do |(k, v), ret|
      key = recursive_key + k.to_s
      if v.is_a? Hash
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
