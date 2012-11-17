require 'thread'
require 'aws/dynamo_db'
require 'scrolls'
require 'l2met/config'
require 'l2met/heartbeat'

module L2met
  module DB
    extend self
    @put_lock = Mutex.new
    @dynamo_lock = Mutex.new
    @table_lock = Mutex.new

    def put(data)
      @put_lock.synchronize do
        Heartbeat.pulse("db-put")
        self["metrics"].put(data)
        self["active-stats"].put(mkey: data[:mkey],
          consumer: data[:consumer],
          time: Time.now.to_i)
      end
    end

    def flush(tname, mkey, bucket)
      Heartbeat.pulse("db-flush")
      t0 = Time.now
      self[tname].query(hash_value: mkey, :select => :all).select do |data|
        data.attributes["time"].to_i == bucket
      end.map do |data|
        data.attributes.tap {data.item.delete}
      end.tap do
        Utils.time(Time.now - t0, ns: 'db', fn: __method__)
      end
    end

    def active_stats(partition, max)
      result = []
      self["active-stats"].each_batch(table_name: 'active-stats') do |b|
        result += b.select do |item|
          Integer(item.attributes["mkey"]) % max == partition
        end.sort_by do |item|
          item.attributes["time"].to_i
        end
      end
      result
    end

    def [](table)
      @table_lock.synchronize do
        tables[table].items
      end
    end

    private

    def tables
      @tables ||= dynamo.tables.
        map {|t| t.load_schema}.
        reduce({}) {|h, t| h[t.name] = t; h}
    end

    def dynamo
      @dynamo_lock.synchronize do
        @db ||= AWS::DynamoDB.new(access_key_id: Config.aws_id,
                                   secret_access_key: Config.aws_secret)
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "db"}.merge(data), &blk)
    end

  end
end
