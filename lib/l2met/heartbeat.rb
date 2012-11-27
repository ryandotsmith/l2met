require 'atomic'
require 'scrolls'

module L2met
  module Heartbeat
    extend self

    def start
      Thread.new do
        loop do
          beats.each do |source, val|
            n = val.swap(0)
            log(at: source, received: n) unless n.zero?
          end
          sleep(1)
        end
      end
    end

    def pulse(source)
      beats[source] ||= Atomic.new(0)
      beats[source].update {|n| n + 1}
    end

    def beats
      @beats ||= {}
    end

    def log(data, &blk)
      Scrolls.log({ns: "heartbeat"}.merge(data), &blk)
    end
  end
end
