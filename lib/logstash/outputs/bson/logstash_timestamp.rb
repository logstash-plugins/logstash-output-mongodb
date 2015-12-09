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
  module LogStashTimestamp

    # A time is type 0x09 in the BSON spec.
    BSON_TYPE = 9.chr.force_encoding(BINARY).freeze

    def to_bson(encoded = ''.force_encoding(BINARY))
      time.to_bson(encoded)
    end

    module ClassMethods
      # Deserialize UTC time from BSON.
      # @param [ BSON ] bson encoded time.
      # @return [ ::LogStash::Timestamp ] The decoded UTC time as a ::LogStash::Timestamp.
      # @see http://bsonspec.org/#/specification
      def from_bson(bson)
        seconds, fragment = BSON::Int64.from_bson(bson).divmod(1000)
        new(::Time.at(seconds, fragment * 1000).utc)
      end
    end

    # Register this type when the module is loaded.
    Registry.register(BSON_TYPE, ::LogStash::Timestamp)
  end

  # Enrich the core LogStash::Timestamp class with this module.
  ::LogStash::Timestamp.send(:include, LogStashTimestamp)
  ::LogStash::Timestamp.send(:extend, LogStashTimestamp::ClassMethods)
end
