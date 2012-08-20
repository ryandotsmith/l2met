require 'l2met/config'
require 'l2met/db'
require 'l2met/stats'
require 'librato/metrics'
require 'scrolls'

module L2met
  module DBOutlet
    extend self

    def start
      loop do
        Thread.new {snapshot; drain}
        sleep(30)
      end
    end

    def drain
      ql = lm_queue.length
      log(fn: __method__, length: ql) do
        if ql > 0
          lm_queue.submit
        end
      end
    end

    def snapshot
      log(fn: __method__) do
        snapshot_counters
        snapshot_histograms
      end
    end

    def snapshot_counters
      DB.flush("counters").group_by do |counter|
        counter["mkey"]
      end.map do |mkey, counters|
        {name: counters.sample["name"],
          source: counters.sample["source"],
          value: counters.map {|c| c["value"].to_f}.reduce(:+)}
      end.each do |metric|
        lm_queue.add(name => {source: metric[:source], type: "gauge",
                       value: metric[:value], measure_time: Time.now.to_i})
      end
    end

    def snapshot_histograms
      DB.flush("histograms").group_by do |hist|
        hist["mkey"]
      end.map do |mkey, hists|
        [{name: hists.sample["name"],
            source: hists.sample["source"]},
          {mean: Stats.mean(hists.map {|h| h["mean"]}),
            median: Stats.median(hists.map {|h| h["median"]}),
            min: Stats.min(hists.map {|h| h["min"]}),
            max: Stats.max(hists.map {|h| h["max"]}),
            perc95: Stats.perc95(hists.map {|h| h["perc95"]}),
            perc99: Stats.perc99(hists.map {|h| h["perc99"]})}]
      end.each do |res|
        meta, metrics = *res
        metrics.each do |stat, val|
          name = [meta[:name], stat].map(&:to_s).join(".")
          lm_queue.add(name => {source: meta[:source], type: "gauge",
                         value: val, measure_time: Time.now.to_i})
        end
      end
    end

    def lm_queue
      @lm_queue ||= Librato::Metrics::Queue.new(client: test_client)
    end

    def test_client
      @client ||= Librato::Metrics::Client.new.
        authenticate(Config.test_librato_email, Config.test_librato_token)
    end

    def log(data, &blk)
      Scrolls.log({ns: "dboutlet"}.merge(data), &blk)
    end
  end
end
