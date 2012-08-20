require 'l2met/db'
require 'scrolls'

module L2met
  module DBOutlet
    extend self

    def start
      loop do
        snapshot_counters
        snapshot_histograms
        sleep(30)
      end
    end

    def snapshot_counters
      DB.flush("counters").each do |counter|
        log(fn: __method__, counter: counter)
      end
    end

    def snapshot_histograms
      DB.flush("histograms").each do |hist|
        log(fn: __method__, histogram: hist)
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "dboutlet"}.merge(data), &blk)
    end
  end
end
