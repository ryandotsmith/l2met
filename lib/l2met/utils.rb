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

    def count(val, name, verbose=false)
      name = Config.app_name + '.' + name
      log(measure: name, val: val) if verbose
      Register.accept(name, val,
        type: "counter",
        source: Config.app_name,
        consumer: Config.l2met_consumer,
        time: Time.now.to_i)
    end

    def time(elapsed, name, verbose=false)
      name = Config.app_name + '.' + name
      log(measure: name, val: elapsed) if verbose
      Register.accept(name, Float(elapsed),
        type: "list",
        source: Config.app_name,
        consumer: Config.l2met_consumer,
        charting: true,
        time: Time.now.to_i)
    end

    def measure(name, verbose=false)
      t0 = Time.now
      yield.tap do
        count(1, name, verbose)
        time(Time.now-t0, name, verbose)
      end
    end

    def trim(s)
      res = s.to_s.
        gsub("/","-").
        gsub(/[^A-Za-z0-9.:\-_]/, '').
        gsub(/^-/,'').
        downcase
      res.empty? ? "root" : res
    end

    def log(data, &blk)
      Scrolls.log({ns: "utils"}.merge(data), &blk)
    end

  end
end
