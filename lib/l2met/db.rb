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

    def put(tname, mkey, uuid, value, opts)
      @put_lock.synchronize do
        Heartbeat.pulse("db-put")
        data = opts.merge(mkey: mkey, uuid: uuid, value: value)
        DB[tname].put(data)
        data = {mkey: mkey, consumer: opts[:consumer], time: Time.now.to_i}
        DB["active-stats"].put(data)
      end
    end

    def flush(tname, mkey, bucket)
      Heartbeat.pulse("db-flush")
      DB[tname].query(hash_value: mkey, :select => :all).map do |data|
        data.attributes
      end.select do |data|
        data["time"].to_i == bucket
      end
    end

    def active_stats(partition, max)
      DB["active-stats"].select.select do |item|
        Integer(item.attributes["mkey"]) % max == partition
      end.sort_by do |item|
        item.attributes["last_report"]
      end
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
