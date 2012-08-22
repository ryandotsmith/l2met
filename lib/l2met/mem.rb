require 'atomic'
require 'scrolls'
require 'l2met/heartbeat'

module L2met
  module Mem
    extend self

    HISTOGRAM_DEFAULTS = {attrs: {display_units_long: "ms"}}
    COUNTER_DEFAULTS = {attrs: {display_units_long: "txn"}}

    def handle(data)
      if data.key?("measure")
        if data.key?("elapsed")
          name = [data["app"], data["fn"]].compact.join(".")
          histogram(name, data["elapsed"].to_f, source: data["source"],
                     consumer: data["consumer"])
          counter(name, 1, source: data["source"], consumer: data["consumer"])
        end
        if data.key?("at") && !["start", "finish"].include?(data["at"])
          name = [data["app"], data["at"]].compact.join(".")
          counter(name, 1, source: data["source"], consumer: data["consumer"])
        end
      end
    end

    def histogram(name, val, opts)
      k = key(name, opts[:source])
      data[:histograms].update do |hash|
        data = {name: name}.merge(opts).merge(HISTOGRAM_DEFAULTS)
        hash[k] ||= data
        hash[k][:values] ||= []
        hash[k][:values] << val
        hash
      end
    end

    def counter(name, val, opts)
      k = key(name, opts[:source])
      data[:counters].update do |hash|
        data = {name: name}.merge(opts).merge(COUNTER_DEFAULTS)
        hash[k] ||= data
        hash[k][:value] ||= 0
        hash[k][:value] += val
        hash
      end
    end

    def histograms
      get(:histograms)
    end

    def histograms!
      flush(:histograms)
    end

    def counters
      get(:counters)
    end

    def counters!
      flush(:counters)
    end

    private

    def key(name, source)
      Digest::SHA1.hexdigest(name, source)
    end

    def flush(type)
      data[type].swap({})
    end

    def get(type)
      data[type].value
    end

    def data
      @data ||= {counters: Atomic.new({}), histograms: Atomic.new({})}
    end

    def log(data)
      t0 = Time.now
      ret = yield
      Scrolls.log({ns: "mem", elapsed: (Time.now - t0)}.merge(data))
      ret
    end
  end
end
