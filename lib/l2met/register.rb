require 'atomic'
require 'scrolls'
require 'l2met/utils'

module L2met
  module Register
    extend self
    TTL = 60

    def start(interval=1)
      Thread.new do
        loop do
          mem[Utils.trunc_time(Time.now)] ||= Atomic.new({})
          mem.delete_if {|k,v| k < (Time.now.to_i - TTL)}
          sleep(interval)
        end
      end
    end

    def accept(name, val, meta)
      mkey = Utils.enc_key(name, meta[:source], meta[:consumer])
      bucket = Utils.trunc_time(meta[:time])
      if mem.key?(bucket)
        mem[bucket].update do |h|
          h[mkey] ||= {name: name}.merge(meta)
          case meta[:type]
          when 'list'
            h[mkey][:value] ||= []
            h[mkey][:value] << val
          when 'counter'
            h[mkey][:value] ||= 0
            h[mkey][:value] += val
          when 'last'
            h[mkey][:value] = val
          end
          h
        end
      else
        log(fn: __method__, at: "drop")
      end
    end

    def snapshot!(bucket)
      if mem.key?(bucket)
        mem[bucket].swap({})
      else
        log(at: "empty-snapshot", bucket: bucket, data: mem)
        {}
      end
    end

    def mem
      @mem ||= {}
    end

    def log(data, &blk)
      Scrolls.log({ns: "register"}.merge(data), &blk)
    end

  end
end
