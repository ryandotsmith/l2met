require 'sequel'
require 'l2met/config'
require 'l2met/utils'

module L2met
  module Outlet
    module Postgres
      extend self

      def get(name, from, to, resolution)
        result = {}
        query(name, from, to, (resolution || 'minute')).each do |metric|
          n, t = metric.delete(:name), metric.delete(:bucket)
          metric.each do |stat, val|
            result[[n, stat].join('.')] ||= []
            result[[n, stat].join('.')] << {x: Integer(t), y: Float(val)}
          end
        end
        result.reduce([]) do |memo, metric|
          memo << {name: metric.first, data: metric.last}
        end
      end

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
          Utils.measure('postgres.insert') do
            metrics_table.insert(s.merge(bucket: Time.at(bucket).utc, name: name))
          end
        end
      end

      private

      def query(name, from, to, resolution)
        valid_resolutions = %w(minute hour day week month)
        if !valid_resolutions.include?(resolution)
          raise(ArgumentError, "Resolution must be one of: #{valid_resolutions.join(', ')}.")
        end
        follower.fetch(<<-EOD).all
          select
            name,
            date_trunc('#{resolution}', bucket) as bucket,
            sum(count) as count,
            avg(mean) as mean
          from
            metrics
          where
            name like '#{name}%' and
            bucket > '#{Time.at(Utils.trunc_time(from.utc))}'::timestamptz and
            bucket < '#{Time.at(Utils.trunc_time(to.utc))}'::timestamptz
          group by name, date_trunc('#{resolution}', bucket)
          order by date_trunc('#{resolution}', bucket) asc
        EOD
      end

      def metrics_table
        @metrics_table ||= pg[:metrics]
      end

      def pg
        @pg ||= Sequel.connect(Config.database_url)
      end

      def follower
        @follower ||= Sequel.connect(Config.read_database_url)
      end

      def log(data, &blk)
        Scrolls.log({ns: "postgres-outlet"}.merge(data), &blk)
      end

    end
  end
end
