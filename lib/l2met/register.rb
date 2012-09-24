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
      puts name
      mkey = Utils.enc_key(name, meta[:source], meta[:consumer])
      bucket = Utils.trunc_time(meta[:time])
      if mem.key?(bucket)
        Heartbeat.pulse("accept")
        mem[bucket].update do |h|
          h[mkey] ||= {name: name}.merge(meta)
          case meta[:type]
          when 'list'
            h[mkey][:value] ||= []
            h[mkey][:value] << val
          when 'counter'
            h[mkey][:value] ||= 0
            h[mkey][:value] += val
          when 'last'
            h[mkey][:value] = val
          end
          h
        end
      else
        log(fn: __method__, at: "drop", name: name, val: val, meta: meta,
          bucket: bucket, buckets: mem.keys)
      end
    end

    def flush
      mem.each do |bucket, ref|
        metrics = ref.swap({})
        metrics.each do |mkey, metric|
          if metric[:value].respond_to?(:sort)
            vals = metric[:value].sort
            DB.put('metrics', mkey, SecureRandom.uuid, 0, {
              time: bucket,
              name: metric[:name],
              type: metric[:type],
              source: metric[:source],
              consumer: metric[:consumer]}.merge(Stats.all(vals)))
          else
            DB.put('metrics', mkey, SecureRandom.uuid, metric[:value],
              time: bucket,
              name: metric[:name],
              type: metric[:type],
              source: metric[:source],
              consumer: metric[:consumer])
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
