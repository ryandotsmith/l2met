require 'scrolls'
require 'securerandom'

require 'l2met/db'
require 'l2met/register'
require 'l2met/stats'
require 'l2met/utils'

module L2met
  module MemOutlet
    extend self
    INTERVAL = 10

    def start
      Thread.new do
        loop do
          m = Utils.trunc_time(Time.now) - 60
          Thread.new {snapshot(m)}
          sleep(INTERVAL)
        end
      end
    end

    def snapshot(m)
      log(fn: __method__, time: m) do
        Register.snapshot!(m).each do |mkey, metric|
          if metric[:value].respond_to?(:sort)
            vals = metric[:value].sort
            data = {
              min: Stats.min(vals),
              max: Stats.max(vals),
              mean: Stats.mean(vals),
              median: Stats.median(vals),
              perc95: Stats.perc95(vals),
              perc99: Stats.perc99(vals)}
            DB.put('metrics', mkey, SecureRandom.uuid, 0, {
              time: m,
              name: metric[:name],
              type: metric[:type],
              source: metric[:source],
              consumer: metric[:consumer]}.merge(data))
          else
            DB.put('metrics', mkey, SecureRandom.uuid, metric[:value],
              time: m,
              name: metric[:name],
              type: metric[:type],
              source: metric[:source],
              consumer: metric[:consumer])
          end
        end
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "mem-outlet"}.merge(data), &blk)
    end

  end
end
