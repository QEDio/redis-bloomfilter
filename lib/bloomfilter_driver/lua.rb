require "digest/sha1"
class Redis
  module BloomfilterDriver

    # It loads lua script into redis.
    # BF implementation is done by lua scripting
    # The alghoritm is executed directly on redis
    # Credits for lua code goes to Erik Dubbelboer
    # https://github.com/ErikDubbelboer/redis-lua-scaling-bloom-filter
    class Lua
      attr_accessor :redis

      def initialize(options = {})
        @options = options
        @redis = @options[:redis]
        lua_load
      end

      def insert(data)
        set data, 1
      end

      def remove(data)
        set data, 0
      end

      def include?(key)
        is_array = Array.try_convert(key) ? true : false
        arr_key = is_array ? key : [key]
        hsh_key = {}

        arr_key.each do |k|
          hsh_key[k] = {key: k, future: 0}
        end

        @redis.pipelined do
          hsh_key.each_pair do |k,v|
            v[:future] = @redis.evalsha(@check_fnc_sha, :keys => [@options[:key_name]], :argv => [@options[:size], @options[:error_rate], k])
          end
        end

        in_filter = []
        hsh_key.each_pair do |k,v|
          # if we have a zero in our result array we (most likely) havent seen this value yet
          # if we don't have a zero in our result array we (most likely) have seen this value already
          in_filter << k if v[:future].value == 1
        end

        # handle single element case
        unless is_array
          if in_filter.length == 1
            return true
          else
            return false
          end
        end

        return in_filter
      end

      def clear
        @redis.keys("#{@options[:key_name]}:*").each {|k|@redis.del k}    
      end

      protected
        # It loads the script inside Redis
        # Taken from https://github.com/ErikDubbelboer/redis-lua-scaling-bloom-filter
        # This is a scalable implementation of BF. It means the initial size can vary
        def lua_load
          add_fnc = %q(
            local entries   = ARGV[1]
            local precision = ARGV[2]
            local set_value = ARGV[4]
            local index     = math.ceil(redis.call('INCR', KEYS[1] .. ':count') / entries)
            local key       = KEYS[1] .. ':' .. index
            local bits = math.floor(-(entries * math.log(precision * math.pow(0.5, index))) / 0.480453013)
            local k = math.floor(0.693147180 * bits / entries)
            local hash = redis.sha1hex(ARGV[3])
            local h = { }
            h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
            h[1] = tonumber(string.sub(hash, 8 , 16), 16)
            h[2] = tonumber(string.sub(hash, 16, 24), 16)
            h[3] = tonumber(string.sub(hash, 24, 32), 16)
            for i=1, k do
              redis.call('SETBIT', key, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, set_value)
            end
          )
          
          check_fnc = %q(

            local entries   = ARGV[1]
            local precision = ARGV[2]
            local index     = redis.call('GET', KEYS[1] .. ':count')
            if not index then
              return 0
            end
            index     = math.ceil(redis.call('GET', KEYS[1] .. ':count') / entries)
            local hash = redis.sha1hex(ARGV[3])
            local h = { }
            h[0] = tonumber(string.sub(hash, 0 , 8 ), 16)
            h[1] = tonumber(string.sub(hash, 8 , 16), 16)
            h[2] = tonumber(string.sub(hash, 16, 24), 16)
            h[3] = tonumber(string.sub(hash, 24, 32), 16)
            local maxk = math.floor(0.693147180 * math.floor((entries * math.log(precision * math.pow(0.5, index))) / -0.480453013) / entries)
            local b    = { }
            for i=1, maxk do
              table.insert(b, h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)])
            end
            for n=1, index do
              local key   = KEYS[1] .. ':' .. n
              local found = true
              local bits = math.floor((entries * math.log(precision * math.pow(0.5, n))) / -0.480453013)
              local k = math.floor(0.693147180 * bits / entries)

              for i=1, k do
                if redis.call('GETBIT', key, b[i] % bits) == 0 then
                  found = false
                  break
                end
              end

              if found then
                return 1
              end
            end

            return 0
          )
          
          @add_fnc_sha   = Digest::SHA1.hexdigest(add_fnc)
          @check_fnc_sha = Digest::SHA1.hexdigest(check_fnc)

          loaded = @redis.script(:exists, [@add_fnc_sha, @check_fnc_sha]).uniq
          if loaded.count != 1 || loaded.first != true
            @add_fnc_sha   = @redis.script(:load, add_fnc)
            @check_fnc_sha = @redis.script(:load, check_fnc)
          end
        end

        def set(data, val)
          arr_data = Array.try_convert(data) || [data]
          @redis.pipelined do
            arr_data.each do |d|
              @redis.evalsha(@add_fnc_sha, :keys => [@options[:key_name]], :argv => [@options[:size], @options[:error_rate], d, val])
            end
          end
        end
    end
  end
end