require 'timeout'
require 'excon'
require 'l2met/config'
require 'l2met/utils'

module L2met
  module Outlet
    module M2pg
      extend self

      def publish(bucket, metrics)
        metrics.each do |mkey, measurements|
          # Only support lists for now.
          # We can get the counter from the list length.
          next if measurements.any? {|m| m['type'] == 'counter'}
          name = measurements.sample['name']
          log(fn: __method__, bucket: Time.at(bucket).min, metric: name)
          s = Stats.aggregate(measurements).merge(count: measurements.length)
          Utils.measure('m2pg.post') do
            post(s.merge(bucket: Time.at(bucket).utc, name: name))
          end
        end
      end

      private

      def post(data)
        Timeout::timeout(1) do
          m2pg_conn.post(path: '/metrics', body: Utils.enc_j(data))
        end
      end

      def m2pg_conn
        @m2pg_conn ||= Excon.new(Config.m2pg_url)
      end

      def log(data, &blk)
        Scrolls.log({ns: "postgres-outlet"}.merge(data), &blk)
      end

    end
  end
end
