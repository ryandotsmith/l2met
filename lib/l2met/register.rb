require 'securerandom'
require 'atomic'
require 'scrolls'

require 'l2met/utils'
require 'l2met/db'
require 'l2met/stats'

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
        name = [Config.app_name, "l2met.register.drop"].join(".")
        accept(name, 1,
          halt: true,
          source: Config.app_name,
          consumer: Config.l2met_consumer,
          type: "counter",
          time: Time.now)
        log(fn: __method__, at: "drop", name: name, val: val, meta: meta,
          bucket: bucket, buckets: mem.keys)
      else
        raise("Register caught in accept loop")
      end
    end

    def flush
      mem.each do |bucket, ref|
        metrics = ref.swap({})
        metrics.each do |mkey, metric|
          data = {mkey: mkey,
            uuid: SecureRandom.uuid,
            time: bucket,
            name: metric[:name],
            type: metric[:type],
            source: metric[:source],
            consumer: metric[:consumer],
            label: metric[:label]}
          if metric[:value].respond_to?(:sort)
            DB.put(data.merge(Stats.all(metric[:value])))
          else
            DB.put(data.merge(value: metric[:value]))
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

    def log(data, &blk)
      Scrolls.log({ns: "register"}.merge(data), &blk)
    end

  end
end
