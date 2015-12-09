# Copyright (C) 2009-2014 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Modified 2015 Elastic

module BSON

  # Injects behaviour for encoding and decoding time values to
  # and from raw bytes as specified by the BSON spec.
  #
  # @see http://bsonspec.org/#/specification
  module LogStashEvent

    # An Event is an embedded document is type 0x03 in the BSON spec..
    BSON_TYPE = 3.chr.force_encoding(BINARY).freeze

    # Get the event as encoded BSON.
    # @example Get the hash as encoded BSON.
    #   Event.new("field" => "value").to_bson
    # @return [ String ] The encoded string.
    # @see http://bsonspec.org/#/specification
     def to_bson(buffer = ByteBuffer.new)
      position = buffer.length
      buffer.put_int32(0)
      to_hash.each do |field, value|
        buffer.put_byte(value.bson_type)
        buffer.put_cstring(field.to_bson_key)
        value.to_bson(buffer)
      end
      buffer.put_byte(NULL_BYTE)
      buffer.replace_int32(position, buffer.length - position)
    end

    # Converts the event to a normalized value in a BSON document.
    # @example Convert the event to a normalized value.
    #   event.to_bson_normalized_value
    # @return [ BSON::Document ] The normalized event.
    def to_bson_normalized_value
      Document.new(self)
    end

    module ClassMethods
      # Deserialize the Event from BSON.
      # @param [ ByteBuffer ] buffer The byte buffer.
      # @return [ Event ] The decoded bson document.
      # @see http://bsonspec.org/#/specification
      def from_bson(buffer)
        hash = Hash.new
        buffer.get_int32 # Throw away the size.
        while (type = buffer.get_byte) != NULL_BYTE
          field = buffer.get_cstring
          hash.store(field, BSON::Registry.get(type).from_bson(buffer))
        end
        new(hash)
      end
    end

    # Register this type when the module is loaded.
    Registry.register(BSON_TYPE, ::LogStash::Event)
  end

  # Enrich the core LogStash::Event class with this module.
  ::LogStash::Event.send(:include, ::LogStashEvent)
  ::LogStash::Event.send(:extend, ::LogStashEvent::ClassMethods)
end
