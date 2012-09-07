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
      if Config.dynamo?
        @put_lock.synchronize do
          Heartbeat.pulse("db-put")
          data = opts.merge(mkey: mkey, uuid: uuid, value: value)
          DB[tname].put(data)
          DB["active-stats"].
            put(mkey: mkey, consumer: opts[:consumer], time: Time.now.to_s)
        end
      end
    end

    def flush(tname, mkey)
      if Config.dynamo?
        Heartbeat.pulse("db-flush")
        DB[tname].query(hash_value: mkey, :select => :all).map do |data|
          data.attributes.tap {|i| data.item.delete}
        end
      end
    end

    def active_stats(partition)
      DB["active-stats"].select.select do |item|
        item.attributes["mkey"] % Config.num_dboutlets == partition
      end.sort_by do |item|
        item.attributes["last_report"]
      end
    end

    def lock(name)
      log(fn: __method__, name: name) do
        begin
          DB["locks"].put({name: name, locked_at: Time.now.to_i},
                           unless_exists: "locked_at")
          log(at: "lock-success")
          true
        rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
          log(at: "lock-failed")
          false
        end
      end
    end

    def unlock(name)
      log(fn: __method__, name: name) do
        DB["locks"].at(name).delete
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
