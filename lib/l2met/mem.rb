require 'atomic'
require 'scrolls'
require 'l2met/db'

module L2met
  module Mem
    extend self

    HISTOGRAM_DEFAULTS = {attrs: {display_units_long: "ms"}}
    COUNTER_DEFAULTS = {attrs: {display_units_long: "txn"}}

    def handle(data)
      counter('l2met.receiver', 1, source: 'mem')
      if data.key?("measure")
        if data.key?("elapsed")
          name = [data["app"], data["fn"]].compact.join(".")
          histogram(name, data["elapsed"].to_f, source: data["source"])
          counter(name, 1, source: data["source"])
        end
        if data.key?("at") && !["start", "finish"].include?(data["at"])
          name = [data["app"], data["at"]].compact.join(".")
          counter(name, 1, source: data["source"])
        end
      end
    end

    def histogram(name, val, opts)
      log(fn: __method__) do
        k = key(name, opts[:source])
        DB.update('histograms', k, Array(val), opts)
        data[:histograms].update do |hash|
          data = {name: name}.merge(opts).merge(HISTOGRAM_DEFAULTS)
          hash[k] ||= data
          hash[k][:values] ||= []
          hash[k][:values] << val
          hash
        end
      end
    end

    def counter(name, val, opts)
      log(fn: __method__) do
        k = key(name, opts[:source])
        DB.update('counters', k, Integer(1), opts)
        data[:counters].update do |hash|
          data = {name: name}.merge(opts).merge(COUNTER_DEFAULTS)
          hash[k] ||= data
          hash[k][:value] ||= 0
          hash[k][:value] += val
          hash
        end
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
