require 'thread'
require 'aws/dynamo_db'
require 'scrolls'
require 'l2met/config'

module L2met
  module DB
    extend self

    @dynamo_semaphore = Mutex.new

    def getp_item!(table, id, args)
      t = get_table(table)
      begin
        t.items.create(args.merge(id: id), unless_exists: "id")
      rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
      end
      t.items.at(id)
    end

    def get_table(name)
      tables.find {|t| t.name == name}
    end

    def tables
      @tables ||= dynamo.tables.map {|t| t.load_schema}
    end

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
