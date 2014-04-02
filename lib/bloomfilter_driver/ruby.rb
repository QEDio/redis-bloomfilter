require "digest/sha1"
class Redis
  module BloomfilterDriver
    class Ruby
      
      # Faster Ruby version.
      # This driver should be used if Redis version < 2.6
      attr_accessor :redis
      def initialize(options = {})
        @options = options
      end

      # Insert a new element
      def insert(data) 
        set data, 1
      end

      # It checks if a key is part of the set
      def include?(key)
        arr_key = Array.try_convert(key) || [key]
        hsh_key = {}

        arr_key.each do |k|
          hsh_key[k] = {key: k, future: [], indexes: indexes_for(k) }
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
              puts "i: #{i}"
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

      # It removes an element from the filter
      def remove(data)
        set data, 0
      end

      # It deletes a bloomfilter
      def clear
        @redis.del @options[:key_name]
      end

      protected
        # Hashing strategy: 
        # http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
        def indexes_for data
          sha = Digest::SHA1.hexdigest(data.to_s)
          h = []
          h[0] = sha[0...8].to_i(16)
          h[1] = sha[8...16].to_i(16)
          h[2] = sha[16...24].to_i(16)
          h[3] = sha[24...32].to_i(16)
          idxs = []

          (@options[:hashes]).times {|i|
            v = (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % @options[:bits]
            idxs << v
          }
          idxs
        end

        def set(key, val)
          arr_key = Array.try_convert(key) || [key]

          @redis.pipelined do
            arr_key.each do |k|
              indexes_for(k).each {|i| @redis.setbit @options[:key_name], i, val}
            end
          end
        end
    end
  end
end