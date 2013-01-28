require 'securerandom'
require 'atomic'
require 'scrolls'
require 'redis'

require 'l2met/utils'
require 'l2met/stats'
require 'l2met/config'

module L2met
  module Register
    extend self
    TTL = 60 * 3

    def start(interval=10)
      Thread.new do
        loop {flush; grow; shrink; sleep(interval)}
      end
    end

    def accept(name, val, meta)
      begin
        mkey = Utils.enc_key(name, meta[:source], meta[:consumer], meta[:type])
        bucket = Utils.trunc_time(meta[:time])
        if mem.key?(bucket)
          Heartbeat.pulse("accept")
          mem[bucket].update do |h|
            h[mkey] ||= {name: name}.merge(meta)
            case meta[:type]
            when 'list'
              h[mkey][:value] ||= []
              h[mkey][:value] << val
              h[mkey][:label] ||= 'time'
            when 'counter'
              h[mkey][:value] ||= 0
              h[mkey][:value] += val
              h[mkey][:label] ||= 'count'
            when 'last'
              h[mkey][:value] = val
              h[mkey][:label] ||= 'last'
            end
            h
          end
        elsif !meta.key?(:halt)
          # Use l2met to measure l2met.
          name = [Config.app_name, "register.drop"].join(".")
          accept(name, 1,
            halt: true,
            source: Config.app_name,
            consumer: Config.l2met_consumer,
            type: "counter",
            time: Time.now)
          log(fn: __method__, at: "drop", name: name, val: val, meta: meta,
            bucket: bucket, buckets: mem.keys)
        else
          $stdout.puts("at=error error=register-caught-in-loop")
        end
      rescue => e
        $stdout.puts("at=error error=#{e.message}")
      end
    end

    def flush
      mem.each do |bucket, ref|
        metrics = ref.swap({})
        metrics.each do |mkey, metric|
          data = {time: metric[:time],
            name: metric[:name],
            type: metric[:type],
            source: metric[:source],
            consumer: metric[:consumer],
            charting: metric[:charting],
            label: metric[:label]}
          if metric[:value].respond_to?(:sort)
            set(mkey, bucket, data.merge(Stats.all(metric[:value])))
          else
            set(mkey, bucket, data.merge(value: metric[:value]))
          end
        end
      end
    end

    private

    def grow
      t = Utils.trunc_time(Time.now)
      3.times {|i| mem[t + (60 * i)] ||= Atomic.new({})}
    end

    def shrink
      mem.delete_if {|k,v| k < (Time.now.to_i - TTL)}
    end

    def mem
      @mem ||= {}
    end

    def set(mkey, bucket, data)
      log(at: "set-bucket", bucket: Time.at(bucket).min, name_source: [data[:name], data[:source].join(".")]) do
        i = [mkey, bucket, SecureRandom.uuid].join(':')
        redis.mapped_hmset(i, data)
        redis.expireat(i, bucket + (3*60))
      end
    end

    def redis
      @redis ||= Redis.new(url: Config.redis_url)
    end

    def log(data, &blk)
      Scrolls.log({ns: "register"}.merge(data), &blk)
    end

  end
end
