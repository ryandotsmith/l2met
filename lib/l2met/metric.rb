require 'atomic'
require 'scrolls'
require 'l2met/db'

module L2met
  module Metric
    extend self

    HISTOGRAM_DEFAULTS = {attrs: {display_units_long: "ms"}}
    COUNTER_DEFAULTS = {attrs: {display_units_long: "txn"}}

    def histogram(name, val, opts)
      k = key(name, opts[:source])
      DB.update('histograms', k, Array(val), opts)
      data[:histograms].update do |hash|
        hash[k] ||= opts.merge(HISTOGRAM_DEFAULTS)
        hash[k][:values] ||= []
        hash[k][:values] << val
        hash
      end
    end

    def counter(name, val, opts)
      k = key(name, opts[:source])
      DB.update('counters', k, Integer(1), opts)
      data[:counters].update do |hash|
        hash[k] ||= opts.merge(COUNTER_DEFAULTS)
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

    def log(data, &blk)
      Scrolls.log({ns: "metric"}.merge(data), &blk)
    end
  end
end
