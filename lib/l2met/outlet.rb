require 'l2met/config'
require 'l2met/metric'
require 'atomic'
require 'scrolls'

module L2met
  module Outlet
    extend self

    def log(data, &blk)
      Scrolls.log({ns: "outlet"}.merge(data), &blk)
    end

    def heartbeat
      log(fn: __method__) do
        @measured = Atomic.new(0)
        Thread.new do
          loop do
            n = @measured.swap(0)
            log(fn: __method__, at: "emit", received: n)
            sleep(1)
          end
        end
      end
    end

    def handle(data)
      if data.key?("measure")
        if data.key?("elapsed")
          name = [data["app"], data["fn"]].compact.join(".")
          Metric.histogram(name: name, source: data["source"],
                         value: data["elapsed"].to_f)
        end
        if data.key?("at") && !["start", "finish"].include?(data["at"])
          name = [data["app"], data["at"]].compact.join(".")
          Metric.counter(name: name, source: data["source"])
        end
        @measured.update {|n| n + 1}
      end
    end

  end
end
