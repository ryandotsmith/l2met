require 'scrolls'
require 'redis'
require 'locksmith/dynamodb'

require 'l2met/db'
require 'l2met/utils'
require 'l2met/config'
require 'l2met/stats'
require 'l2met/outlets/librato'
require 'l2met/outlets/m2pg'

module L2met
  module Outlet
    extend self
    DELAY = 60

    def start
      loop do
        Config.num_outlets.times.each do |p|
          locker.lock("#{Config.app_name}.outlet.#{p}", ttl: 60) do
            bucket = Utils.trunc_time(Time.now) - DELAY
            snapshot(p, Config.num_outlets, bucket)
          end
        end
      end
    end

    def snapshot(partition, max, bucket)
      Utils.measure('outlet.snapshot') do
        # redis layout: mkey:bucket:uuid
        Utils.measure('outlet.redis.key-scan') do
          redis.keys("*:#{bucket}:*")
        end.select do |key|
          Integer(key.split(':')[0]) % max == partition
        end.map do |key|
          mkey = key.split(':')[0]
          Utils.measure('outlet.redis.get') do
            redis.hgetall(key).merge('mkey' => mkey)
          end
        end.group_by do |metric|
          metric["consumer"]
        end.each do |consumer_id, metrics|
          begin
            metrics = metrics.group_by {|m| m['mkey']}
            M2pg.publish(bucket, metrics) if Config.enable_m2pg?
            Librato.publish(consumer_id, bucket, metrics) if Config.enable_librato?
          rescue => e
            Utils.count(1, 'outlet.metric-post-error')
            log(fn: __method__, at: 'error', consumer: consumer_id,
              error: e.inspect)
            next
          end
        end
      end
    end

    def redis
      @redis ||= Redis.new(url: Config.redis_url)
    end

    def locker
      @locker ||= Locksmith::Dynamodb
    end

    def log(data, &blk)
      Scrolls.log({ns: "outlet"}.merge(data), &blk)
    end

  end
end
