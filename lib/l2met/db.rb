require 'thread'
require 'aws/dynamo_db'
require 'scrolls'
require 'l2met/config'

module L2met
  module DB
    extend self
    @put_lock = Mutex.new
    @dynamo_lock = Mutex.new

    def put(tname, id, mkey, value, opts)
      if Config.dynamo?
        @put_lock.synchronize do
          log(fn: __method__, tname: tname) do
            data = opts.merge(id: id, mkey: mkey, value: value)
            log(fn: __method__, at: 'creation', data: data)
            tables[tname].items.put(data)
          end
        end
      end
    end

    def flush(tname)
      if Config.dynamo?
        log(fn: __method__, tname: tname) do
          tables[tname].items.select.map do |data|
            data.attributes.tap {|i| data.item.delete}
          end
        end
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
