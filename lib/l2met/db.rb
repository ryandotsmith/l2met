require 'thread'
require 'aws/dynamo_db'
require 'scrolls'

require 'l2met/config'
require 'l2met/heartbeat'

module L2met
  module DB
    extend self
    @dynamo_lock = Mutex.new
    @table_lock = Mutex.new

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
