require 'l2met/db'
require 'l2met/stats'

module L2met
  module Outlet
    module Dynamodb
      extend self

      def publish(bucket, metrics)
        metrics.each do |mkey, measurements|
          # Set the charting flag on all metrics to use this service.
          next unless measurements.all? {|m| m.key?('charting')}
          # Only support lists for now.
          # We can get the counter from the list length.
          next if measurements.any? {|m| m['type'] == 'counter'}
          name = measurements.sample['name']
          log(fn: __method__, bucket: Time.at(bucket).min, metric: name)
          s = Stats.aggregate(measurements).merge(count: measurements.length)
          chart_data.put(s.merge(time: bucket, name: name))
        end
      end

      def chart_data
        @chart_data ||= DB["#{Config.app_name}.chart-data"]
      end

      def log(data, &blk)
        Scrolls.log({ns: "dynamodb-outlet"}.merge(data), &blk)
      end

    end
  end
end
