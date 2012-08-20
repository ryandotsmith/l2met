require 'l2met/db'
require 'scrolls'

module L2met
  module DBOutlet
    extend self

    def start
      loop do
        sleep(30)
        snapshot_counters
        snapshot_histograms
      end
    end

    def snapshot_counters
      DB.flush("counters").each do |counter|
        log(fn: __method__, counter: counter)
      end
    end

    def snapshot_histograms
      DB.flush("histograms").each do |hist|
        log(fn: __method__, counter: counter)
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "dboutlet"}.merge(data), &blk)
    end
  end
end
