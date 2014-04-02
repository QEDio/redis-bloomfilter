require "digest/md5"
require "digest/sha1"
require "zlib"
class Redis
  module BloomfilterDriver
    # It uses different hash strategy
    # Usefule for benchmarking
    class RubyTest
      attr_accessor :redis

      def initialize(options = {})
        @options = options
      end

      # Insert a new element
      def insert(data) 
        set data, 1
      end

      # Insert a new element
      def remove(data) 
        set data, 0
      end

      # It checks if a key is part of the set
      def include?(key)
        arr_key = Array.try_convert(key) || [key]
        hsh_key = {}

        arr_key.each do |k|
          indexes = []
          indexes_for(k) { |idx| indexes << idx }
          hsh_key[k.to_s] = {key: k, future: [], included: true, indexes:  indexes}
        end

        # if the first bit returned from redis is 0 we don't look at this any further?
        @redis.pipelined do
          hsh_key.each_pair do |k,v|
            v[:future][0] = @redis.getbit(@options[:key_name], v[:indexes].shift)
          end
        end

        @redis.pipelined do
          hsh_key.each_pair do |k,v|
            # filter all that are 0
            next if v[:future][0].value == 0
            v[:indexes].each_with_index do |idx, i|
              v[:future][i+1] = @redis.getbit(@options[:key_name], idx)
            end
          end
        end

        filtered_arr = []
        hsh_key.each_pair do |k,v|
          puts "future: #{v[:future].map{|f|f.value}}"
          filtered_arr << k unless v[:future].map{|f|f.value}.include?(0)
        end

        if arr_key.length == 1
          if filtered_arr.length == 0
            return false
          else
            return true
          end
        end

        return filtered_arr
      end

      # It deletes a bloomfilter
      def clear
        @redis.del @options[:key_name]
      end

      protected
        def indexes_for(key, engine = nil)
          engine ||= @options[:hash_engine]
          @options[:hashes].times do |i|
            yield self.send("engine_#{engine}", key.to_s, i)
          end
        end

        # A set of different hash functions
        def engine_crc32(data, i)
          Zlib.crc32("#{i}-#{data}").to_i(16) % @options[:bits]
        end

        def engine_md5(data, i)
          Digest::MD5.hexdigest("#{i}-#{data}").to_i(16) % @options[:bits]
        end

        def engine_sha1(data, i)
          Digest::SHA1.hexdigest("#{i}-#{data}").to_i(16) % @options[:bits]
        end

        def set(data, val)
          arr_data = Array.try_convert(data) || [data]
          @redis.pipelined do
            arr_data.each do |d|
              indexes_for(d) { |i| @redis.setbit @options[:key_name], i, val }
            end
          end
        end
    end
  end
end