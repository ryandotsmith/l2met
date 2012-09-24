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

    def time(name, t, opts={})
      if name
        name.
          gsub(/\/:\w+/,'').            #remove param names from path
          gsub("/","-").                #remove slash from path
          gsub(/[^A-Za-z0-9\-\_]/, ''). #only keep subset of chars
          slice(1..-1).
          tap {|res| log({measure: true, fn: res, elapsed: t}.merge(opts))}
      end
    end

    def log(data, &blk)
      Scrolls.log({ns: "utils"}.merge(data), &blk)
    end

  end
end
