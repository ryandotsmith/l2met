require 'l2met/config'
require 'l2met/db'
require 'l2met/stats'
require 'librato/metrics'
require 'scrolls'

module L2met
  module DBOutlet
    extend self
    INTERVAL = 60

    def start
      loop do
        sleep(INTERVAL)
        lock_partition("db-outlet") do |p|
          to = Time.now.to_i - 10
          from = (to - 60)
          Thread.new {snapshot(p, from, to)}
        end

      end
    end

    def lock_partition(desc)
      partition = nil
      (0..Config.num_dboutlets).to_a.shuffle.each do |i|
        name = [desc, i].map(&:to_s).join(".")
        if DB.lock(name)
          ret = yield(i)
          DB.unlock(name)
          return ret
        end
      end
    end

    def snapshot(partition, from, to)
      log(fn: __method__, from: from, to: to, partition: partition) do
        DB.active_stats(partition).each do |stat|
          begin
            sa = stat.attributes
            consumer = DB["consumers"].at(sa["consumer"]).attributes.to_h
            client = build_client(consumer["email"], consumer["token"])
            queue =  Librato::Metrics::Queue.new(client: client)
            snapshot_counters!(queue, sa["mkey"].to_i, from, to)
            snapshot_histograms!(queue, sa["mkey"].to_i, from, to)
            snapshot_last_vals!(queue, sa["mkey"].to_i, from, to)
            if queue.length > 0
              queue.submit
            end
          rescue => e
            log(at: "error", error: e.message)
            next
          end
        end
      end
    end

    def snapshot_last_vals!(q, mkey, from, to)
      counters = DB.flush("last_vals", mkey, from, to)
      if counters.length > 0
        sample = counters.last
        q.add(sample["name"] => {source: sample["source"],
                type: "gauge",
                value: sample["value"].to_i,
                measure_time: sample["time"]})
      end
    end

    def snapshot_counters!(q, mkey, from ,to)
      counters = DB.flush("counters", mkey, from, to)
      if counters.length > 0
        sample = counters.sample
        q.add(sample["name"] => {source: sample["source"],
                type: "gauge",
                value: counters.map {|c| c["value"].to_f}.reduce(:+),
                measure_time: sample["time"].to_i})
      end
    end

    def snapshot_histograms!(q, mkey, from, to)
      hists = DB.flush("histograms", mkey, from, to)
      if hists.length > 0
        meta = {name: hists.sample["name"], source: hists.sample["source"]}
        data = {mean: Stats.mean(hists.map {|h| h["mean"]}),
          median: Stats.median(hists.map {|h| h["median"]}),
          min: Stats.min(hists.map {|h| h["min"]}),
          max: Stats.max(hists.map {|h| h["max"]}),
          perc95: Stats.perc95(hists.map {|h| h["perc95"]}),
          perc99: Stats.perc99(hists.map {|h| h["perc99"]})}
        data.each do |stat, val|
          name = [meta[:name], stat].map(&:to_s).join(".")
          q.add(name => {source: meta[:source],
                  type: "gauge",
                  value: val,
                  measure_time: meta["time"]})
        end
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
