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
          mem[Utils.trunc_time(Time.now)] ||= {}
          mem.delete_if {|k,v| k < (Time.now.to_i - TTL)}
          sleep(interval)
        end
      end
    end

    def accept(name, val, meta)
      puts meta
      key = Utils.enc_key(name, meta[:source], meta[:consumer])
      k = Utils.trunc_time(meta[:time])
      type = meta[:type]
      if mem.key?(k)
        mem[k][type] ||= Atomic.new({})
        mem[k][type].update do |h|
          h[key] ||= {name: name}.merge(meta)
          case type
          when 'list'
            h[key][:value] ||= []
            h[key][:value] << val
          when 'counter'
            h[key][:value] ||= 0
            h[key][:value] += val
          when 'last'
            h[key][:value] = val
          end
          h
        end
      else
        log(fn: __method__, at: "drop")
      end
    end

    def snapshot!(m)
      if mem.key?(m)
        mem[m].map do |type, ref|
          {type => ref.swap({})}
        end
      else
        log(at: "empty-snapshot", time: m, data: mem)
        []
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
