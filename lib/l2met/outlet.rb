require 'l2met/config'
require 'metriks'
require 'metriks/reporter/librato_metrics'

module L2met
  module Outlet
    extend self

    def start
      email = Config.librato_email
      token = Config.librato_token
      opts = {interval: 60}
      if email && token
        @reporter = Metriks::Reporter::LibratoMetrics.new(email, token, opts)
        @reporter.start
      end
    end

    def handle(data)
      if data["measure"]
        if data["elapsed"] && data["at"] != "finish"
          name = [data["source"], data["fn"]].compact.join("-")
          Metriks.timer(name).update(data["elapsed"])
        end
        if data.key?("at") && !["start", "finish"].include?(data["at"])
          name = [data["source"], data["at"]].compact.join("-")
          Metriks.meter(name).mark
        end
      end
    end

  end
end
