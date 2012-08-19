require 'thread'
require 'aws/dynamo_db'
require 'scrolls'
require 'l2met/config'

module L2met
  module DB
    extend self

    def update(tname, id, mkey, value, opts)
      if Config.dynamo?
        log(fn: __method__, tname: tname) do
          create(tname, id, mkey, opts).attributes.merge!(value: value)
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

    @create_semaphore = Mutex.new
    def create(tname, id, mkey, opts)
      res = nil
      @create_semaphore.synchronize do
        res = tables[tname].items[id]
        if !res.exists?
          tables[tname].items.create(opts.merge(id: id, mkey: mkey))
        end
      end
      res
    end

    def tables
      @tables ||= dynamo.tables.
        map {|t| t.load_schema}.
        reduce({}) {|h, t| h[t.name] = t; h}
    end

    @dynamo_semaphore = Mutex.new
    def dynamo
      @dynamo_semaphore.synchronize do
        @db ||= AWS::DynamoDB.new(access_key_id: Config.aws_id,
                                   secret_access_key: Config.aws_secret)
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "db"}.merge(data), &blk)
    end

  end
end
