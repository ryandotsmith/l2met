require 'librato/metrics'
require 'scrolls'
require 'locksmith/dynamodb'

require 'l2met/utils'
require 'l2met/config'
require 'l2met/db'
require 'l2met/stats'

module L2met
  module DBOutlet
    extend self
    INTERVAL = 10

    def start
      max = Config.num_dboutlets
      loop do
        bucket = Utils.trunc_time(Time.now) - 120
        Thread.new do
          max.times.each do |p|
            Locksmith::Dynamodb.lock("dboutlet.#{p}") do
              snapshot(p, max, bucket)
            end
          end
        end
        sleep(INTERVAL)
      end
    end

    def snapshot(partition, max, bucket)
      log(fn: __method__, bucket: bucket, time: Time.at(bucket), partition: partition) do
        DB.active_stats(partition, max).each do |stat|
          begin
            sa = stat.attributes
            consumer = DB["consumers"].at(sa["consumer"]).attributes.to_h
            client = build_client(consumer["email"], consumer["token"])
            queue =  Librato::Metrics::Queue.new(client: client)
            flush(sa["mkey"].to_i, bucket).tap do |col|
              Utils.last(col.length, ns: "db-outlet", fn: __method__)
              log(fn: __method__, at: "flush", last: col.length)
            end.each {|m| queue.add(m)}
            queue.submit if queue.length > 0
          rescue => e
            log(at: "error", error: e.message)
            next
          end
        end
      end
    end

    def flush(mkey, bucket)
      log(fn: __method__, mkey: mkey, bucket: bucket, time: Time.at(bucket)) do
        DB.flush("metrics", mkey, bucket).group_by do |metric|
          metric["type"]
        end.map do |type, metrics|
          s = metrics.sample
          opts = {source: s["source"], type: "gauge", measure_time: s["time"].to_i}
          log(fn: __method__, at: "process-#{s["name"]}")
          case type
          when "counter"
            val = metrics.map {|m| m["value"]}.reduce(:+).to_f
            {s["name"] => opts.merge(value: val)}
          when "last"
            val = metrics.last["value"].to_i
            {s["name"] => opts.merge(value: val)}
          when "list"
            Stats.aggregate(metrics).map do |stat, val|
              name = [s["name"], stat].map(&:to_s).join(".")
              {name => opts.merge(value: val)}
            end
          end
        end.flatten.compact
      end
    end

    def build_client(email, token)
      Librato::Metrics::Client.new.tap do |c|
        c.authenticate(email, token)
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "dboutlet"}.merge(data), &blk)
    end

  end
end
