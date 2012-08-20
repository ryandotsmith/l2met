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
      DB.flush("counters")
    end

    def snapshot_histograms
      DB.flush("histograms")
    end

    def log(data, &blk)
      Scrolls.log({ns: "dboutlet"}.merge(data), &blk)
    end
  end
end
