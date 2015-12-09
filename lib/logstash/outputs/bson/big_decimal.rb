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

  # Injects behaviour for encoding and decoding BigDecimal values
  # to and from # raw bytes as specified by the BSON spec.
  #
  # @see http://bsonspec.org/#/specification
  module BigDecimal

    # A floating point is type 0x01 in the BSON spec.
    BSON_TYPE = 1.chr.force_encoding(BINARY).freeze

    # The pack directive is for 8 byte floating points.
    PACK = "E".freeze

    # Get the floating point as encoded BSON.
    # @example Get the floating point as encoded BSON.
    #   1.221311.to_bson
    # @return [ String ] The encoded string.
    # @see http://bsonspec.org/#/specification
    def to_bson(encoded = ''.force_encoding(BINARY))
      encoded << [ self ].pack(PACK)
    end

    module ClassMethods

      # Deserialize an instance of a BigDecimal from a BSON double.
      # @param [ BSON ] bson object from Mongo.
      # @return [ BigDecimal ] The decoded BigDecimal.
      # @see http://bsonspec.org/#/specification
      def from_bson(bson)
        from_bson_double(bson.read(8))
      end

      private

      def from_bson_double(double)
        new(double.unpack(PACK).first.to_s)
      end
    end

    # Register this type when the module is loaded.
    Registry.register(BSON_TYPE, ::BigDecimal)
  end

  # Enrich the core BigDecimal class with this module.
  #
  # @since 2.0.0
  ::BigDecimal.send(:include, BigDecimal)
  ::BigDecimal.send(:extend, BigDecimal::ClassMethods)
end
