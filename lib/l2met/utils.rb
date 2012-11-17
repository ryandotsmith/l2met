require 'zlib'
require 'yajl'
require 'scrolls'

module L2met
  module Utils
    extend self

    def trunc_time(t, offset=60)
      (t.to_i / offset) * offset
    end

    def enc_key(*things)
      Zlib.crc32(things.join)
    end

    def enc_j(data)
      Yajl::Encoder.encode(data)
    end

    def count(val, name)
      Register.accept(name, val,
        type: "counter",
        source: Config.app_name,
        consumer: Config.l2met_consumer,
        time: Time.now)
    end

    def time(elapsed, name)
      Register.accept(name, Float(elapsed),
        type: "list",
        source: Config.app_name,
        consumer: Config.l2met_consumer,
        time: Time.now)
    end

    def measure(name)
      name = Config.app_name + name
      t0 = Time.now
      yield.tap do
        count(1, name)
        time(Time.now-t0, name)
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "utils"}.merge(data), &blk)
    end

  end
end
