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

    def handle(data)
      if data.key?("measure")
        if data.key?("elapsed")
          name = [data["app"], data["fn"]].compact.join(".")
          Metric.histogram(name, data["elapsed"].to_f, source: data["source"])
          Metric.counter(name, 1, source: data["source"])
        end
        if data.key?("at") && !["start", "finish"].include?(data["at"])
          name = [data["app"], data["at"]].compact.join(".")
          Metric.counter(name, 1, source: data["source"])
        end
      end
    end

  end
end
