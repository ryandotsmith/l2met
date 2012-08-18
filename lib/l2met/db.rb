require 'thread'
require 'aws/dynamo_db'
require 'scrolls'
require 'l2met/config'

module L2met
  module DB
    extend self

    def update(tname, id, value, opts)
      if Config.dynamo?
        log(fn: __method__, tname: tname) do
          create(tname, id, opts).
            attributes.
            add(value: value)
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

    def create(tname, id, opts)
      begin
        tables[tname].items.create(opts.merge(id: id), unless_exists: "id")
      rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
        #noop
      ensure
        tables[tname].items.at(id)
      end
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
