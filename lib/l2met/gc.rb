require 'scrolls'
require 'l2met/heartbeat'
require 'l2met/db'

module L2met
  module GC
    extend self
    INTERVAL = 60

    def start
      loop do
        t = Time.now.to_i
        Thread.new {collect(t - (60*60))}
        sleep(INTERVAL)
      end
    end

    def collect(t)
      log(fn: __method__, time: t) do
        DB["active-stats"].each do |item|
          mkey = item.attributes["mkey"]
          %w(counters histograms last_vals).each do |tname|
            log(at: tname)
            DB[tname].query(hash_value: mkey).each do |i|
              if i.attributes["time"].to_i < t
                i.delete
                Heartbeat.pulse("gc-collect-#{tname}")
              end
            end
          end
        end
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "gc"}.merge(data), &blk)
    end

  end
end
