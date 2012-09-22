require 'l2met/config'
require 'l2met/db'
require 'l2met/stats'
require 'librato/metrics'
require 'scrolls'
require 'locksmith/dynamodb'

module L2met
  module DBOutlet
    extend self
    INTERVAL = 60

    def start
      max = Config.num_dboutlets
      loop do
        t = Time.now.to_i - 60
        Thread.new do
          (0..max).to_a.shuffle.each do |p|
            Locksmith::Dynamodb.lock("dboutlet.#{p}") do
              snapshot(p, max, t - INTERVAL, t)
            end
          end
        end
        sleep(INTERVAL)
      end
    end

    def snapshot(partition, max, from, to)
      log(fn: __method__, from: from, to: to, partition: partition) do
        DB.active_stats(partition, max).each do |stat|
          begin
            sa = stat.attributes
            consumer = DB["consumers"].at(sa["consumer"]).attributes.to_h
            client = build_client(consumer["email"], consumer["token"])
            queue =  Librato::Metrics::Queue.new(client: client)
            flush(sa["mkey"].to_i, from, to).each {|m| queue.add(m)}
            queue.submit if queue.length > 0
          rescue => e
            log(at: "error", error: e.message)
            next
          end
        end
      end
    end

    def flush(mkey, from, to)
      DB.flush("metrics", mkey, from, to).group_by do |metric|
        metric["type"]
      end.map do |type, metrics|
        s = metrics.sample
        opts = {source: s["source"],type: "gauge",measure_time: s["time"].to_i}
        case type
        when "counter"
          val = metrics.map {|m| m["value"]}.reduce(:+).to_f
          {s["name"] => opts.merge(value: val)}
        when "last-val"
          val = metrics.last["value"].to_i
          {s["name"] => opts.merge(value: val)}
        when "histogram"
          Stats.aggregate(metrics).map do |stat, val|
            name = [s["name"], stat].map(&:to_s).join(".")
            {name => opts.merge(value: val)}
          end
        end
      end.flatten
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
