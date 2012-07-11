require 'mongo'

module Cachely
  def self.factory backend, options = {}
    backend = Cachely::Backend.const_get(backend.to_str.capitalize!) unless backend.is_a? Class
    backend.new options
  end
  
  class Backend
    class Core
      def initialize options = {}
        @options = defaults.merge! options
      end

      def defaults
        {
          cache_id_prefix:            nil,
          lifetime:                   3600,
          automatic_serialization:    true
        }
      end
      
      def save obj, id, tags = [], lifetime = nil
        true
      end
      
      def calc_expires ttl = @options[:lifetime]
        return nil if ttl.nil?
        Time.now + ttl
      end
    end
    
    class Mongo < Cachely::Backend::Core
      def defaults
        super.merge!({
          host:                       'localhost',
          port:                       '27017',
          dbname:                     'cachely',
          collection:                 'cache',
          config:                     {} # empty
        })
      end

      def save data, id, tags = [], ttl = @options[:lifetime]
        collection.ensure_index([[:tags, 1]])
        collection.ensure_index([[:expires, -1]])
        collection.save({
          _id:        id,
          data:       data,
          tags:       tags,
          expires:    calc_expires(ttl)
        })
      end
      
      def load id
        doc = collection.find_one(_id: id)
        return nil if doc.nil?
        if (doc["expires"] && doc["expires"] < Time.now)
          remove id
          return nil
        end
        doc["data"]
      end
      
      def remove id
        collection.remove(_id: id)
      end
      
      def clean_tags tags
        collection.remove(tags: tags)
      end
      
      def clean_expired
        collection.remove(expires: {'$lt' => Time.now})
      end
      
      def clean
        collection.drop
      end
      
      private
      
      def collection
        @collection ||= db[@options[:collection]]
      end
      
      def db
        @db ||= connection[@options[:dbname]]
      end
      
      def connection
        @connection ||= connect
      end
      
      def connect
        ::Mongo::Connection.new(@options[:host], @options[:port], @options[:config])
      end
    end
  end
end