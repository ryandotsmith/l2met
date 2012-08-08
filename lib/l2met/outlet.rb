require 'l2met/config'
require 'metriks'
require 'metriks/reporter/librato_metrics'

module L2met
  module Outlet
    extend self

    def heartbeat
      log(fn: "heartbeat") do
        @measured = Atomic.new(0)
        Thread.new do
          loop do
            n = @measured.swap(0)
            log(fn: "heartbeat", at: "emit", received: n)
            sleep(1)
          end
        end
      end
    end

    def start
      heartbeat
      email = Config.librato_email
      token = Config.librato_token
      opts = {interval: 60}
      if email && token
        @reporter = Metriks::Reporter::LibratoMetrics.new(email, token, opts)
        @reporter.start
      end
    end

    def handle(data)
      if data.key?("measure")
        if data.key?("elapsed")
          name = [data["app"], data["fn"]].compact.join(".")
          Metriks.timer(name).update(data["elapsed"].to_f)
        end
        if data.key?("at") && !["start", "finish"].include?(data["at"])
          name = [data["app"], data["at"]].compact.join(".")
          Metriks.meter(name).mark
        end
        @measured.update {|n| n + 1}
      end
    end

    def self.log(data, &blk)
      Scrolls.log({ns: "outlet"}.merge(data), &blk)
    end

  end
end
