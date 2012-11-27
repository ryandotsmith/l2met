require 'librato/metrics'
require 'l2met/db'
require 'l2met/utils'

module L2met
  module Outlet
    module Librato
      extend self

      def publish(consumer_id, bucket, metrics)
        q = ::Librato::Metrics::Queue.new(client: librato_client(consumer_id))
        aggregate(bucket, metrics).each {|m| q.add(m)}
        if q.length > 0
          Utils.count(q.length, 'outlet.librato.metrics')
          Utils.measure('outlet.librato.submit') {q.submit}
        end
      end

      def aggregate(bucket, metrics)
        metrics.map do |mkey, metrics|
          s = metrics.sample
          opts = {source: s["source"],
            type: "gauge",
            attributes: {display_units_long: s["label"]},
            measure_time: bucket}
          log(fn: __method__, at: "process", bucket: Time.at(bucket).min, metric: s["name"])
          case s["type"]
          when "counter"
            val = metrics.map {|m| Float(m["value"])}.reduce(:+)
            name = [s["name"], 'count'].join(".")
            {name => opts.merge(value: val)}
          when "last"
            val = Float(metrics.last["value"])
            name = [s["name"], 'last'].join(".")
            {name => opts.merge(value: val)}
          when "list"
            Stats.aggregate(metrics).map do |stat, val|
              name = [s["name"], stat].map(&:to_s).join(".")
              {name => opts.merge(value: val)}
            end
          end
        end.flatten.compact
      end

      def librato_client(consumer_id)
        consumer = DB["consumers"].at(consumer_id).attributes
        ::Librato::Metrics::Client.new.tap do |c|
          c.authenticate(consumer["email"], consumer["token"])
        end
      end

      def log(data, &blk)
        Scrolls.log({ns: "librato-outlet"}.merge(data), &blk)
      end

    end
  end
end
