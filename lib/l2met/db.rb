require 'thread'
require 'aws/dynamo_db'
require 'scrolls'

require 'l2met/config'
require 'l2met/heartbeat'
require 'l2met/utils'

module L2met
  module DB
    extend self
    @dynamo_lock = Mutex.new
    @table_lock = Mutex.new

    def metrics(name, from, to)
      f = Utils.trunc_time(Time.at(Integer(from)))
      t = Utils.trunc_time(Time.at(Integer(to)))
      result = {}
      while f < t
        q = {hash_value: f += 60, range_begins_with: name}
        items = DB["#{Config.app_name}.chart-data"].query(q)
        data = items.map(&:attributes).map(&:to_h)
        data.each do |metric|
          n, t = metric.delete('name'), metric.delete('time')
          metric.each do |stat, val|
            result[[n, stat].join('.')] ||= []
            result[[n, stat].join('.')] << {x: Integer(t), y: Float(val)}
          end
        end
      end
      result.reduce([]) do |memo, metric|
        memo << {name: metric.first, data: metric.last}
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
