require 'atomic'
require 'aws/dynamo_db'
require 'scrolls'
require 'l2met/config'

module L2met
  module Metric
    extend self

    HISTOGRAM_DEFAULTS = {attrs: {display_units_long: "ms"}}
    COUNTER_DEFAULTS = {attrs: {display_units_long: "txn"}}

    def histogram(args)
      if Config.dynamo?
        getp_item!('histograms', key(args), args).
          attributes.
          add(values: Array(args[:value]))
      end
      data[:histograms].update do |hash|
        k = key(args)
        hash[k] ||= args.merge(HISTOGRAM_DEFAULTS)
        hash[k][:values] ||= []
        hash[k][:values] << args[:value]
        hash
      end
    end

    def counter(args)
      if Config.dynamo?
        getp_item!('counters', key(args), args).
          attributes.
          add(value: 1)
      end
      data[:counters].update do |hash|
        k = key(args)
        hash[k] ||= args.merge(COUNTER_DEFAULTS)
        hash[k][:value] ||= 0
        hash[k][:value] += 1
        hash
      end
    end

    def histograms
      get(:histograms)
    end

    def histograms!
      flush(:histograms)
    end

    def counters
      get(:counters)
    end

    def counters!
      flush(:counters)
    end

    private

    def key(args)
      Digest::SHA1.hexdigest([:name, :source, :lable].map {|k| args[k]}.join)
    end

    def flush(type)
      data[type].swap({})
    end

    def get(type)
      data[type].value
    end

    def data
      @data ||= {counters: Atomic.new({}), histograms: Atomic.new({})}
    end

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
      @db ||= AWS::DynamoDB.new(access_key_id: Config.aws_id,
                                 secret_access_key: Config.aws_secret)
    end

    def log(data, &blk)
      Scrolls.log({ns: "metric"}.merge(data), &blk)
    end
  end
end
