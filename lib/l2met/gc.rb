require 'scrolls'
require 'l2met/heartbeat'
require 'l2met/db'

module L2met
  module GC
    extend self
    INTERVAL = 60

    def start
      loop do
        t = (Time.now - (60*5)).to_i
        Thread.new {metrics(t); active_stats(t)}
        sleep(INTERVAL)
      end
    end

    def metrics(t)
      log(fn: __method__, time: t) do
        DB["active-stats"].each do |item|
          flush_mkey(item.attributes["mkey"],
                      -> {item.attributes["time"].to_i , t})
        end
      end
    end

    def active_stats(t)
      DB["active-stats"].each do |item|
        if item.attributes["time"].to_i < t
          item.delete
          Heartbeat.pulse("gc-collect-active-stats")
        end
      end
    end

    def flush_mkey(mkey, pred=nil)
      %w(counters histograms last_vals).each do |tname|
        DB[tname].query(hash_value: mkey).each do |i|
          if pred
            i.delete if pred.call
          else
            i.delete
          end
          Heartbeat.pulse("gc-collect-#{tname}")
        end
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "gc"}.merge(data), &blk)
    end

  end
end
