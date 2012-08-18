require 'thread'
require 'aws/dynamo_db'
require 'scrolls'
require 'l2met/config'

module L2met
  module DB
    extend self

    def getp_item!(tname, id, args)
      begin
        tables[tname].items.create(args.merge(id: id), unless_exists: "id")
      rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
      end
      tables[tname].items.at(id)
    end

    private

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
