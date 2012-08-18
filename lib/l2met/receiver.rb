require "l2met/config"
require "l2met/outlet"
require "socket"
require "securerandom"
require "atomic"
require "scrolls"

module L2met
  module Receiver
    LineRe = /^\d+ \<\d+\>1 \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\+00:00 d\.[a-z0-9-]+ ([a-z0-9\-\_\.]+) ([a-z0-9\-\_\.]+) \- \- (.*)$/
    IgnoreMsgRe = /(^ *$)|(Processing|Parameters|Completed|\[Worker\(host)/
    TimeSubRe = / \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d-\d\d:\d\d/
    AttrsRe = /( *)([a-zA-Z0-9\_\-\.]+)=?(([a-zA-Z0-9\.\-\_\.]+)|("([^\"]+)"))?/

    def self.parse_msg(msg)
      if !msg.match(IgnoreMsgRe)
        msg = msg.sub(TimeSubRe, "")
        data = {}
        msg.scan(AttrsRe) do |_, key, _, val1, _, val2|
          if (((key == "service") || (key == "wait")) && val1)
            data[key] = val1.sub("ms", "")
          else
            data[key] = (val1 || val2 || "true")
          end
        end
        data
      end
    end

    def self.parse(line)
      if (m = line.match(LineRe))
        if (data = parse_msg(m[3]))
          data["source"] = m[1]
          data["ps"] = m[2]
          data
        end
      end
    end

    def self.bind
      log(fn: "bind", port: Config.port) do
        @server = TCPServer.new("0.0.0.0", Config.port)
      end
    end

    def self.await
      log(fn: "await") do
        loop do
          Thread.start(@server.accept) do |client|
            drain(client)
          end
        end
      end
    end

    def self.drain(client)
      client_id = SecureRandom.uuid
      log(fn: "drain", client_id: client_id) do
        while line = client.gets
          if data = parse(line.chomp)
            Outlet.handle(data)
            Metric.counter('l2met.receiver', 1, source: 'drain')
          end
        end
      end
    end

    def self.trap
      log(fn: "trap") do
        ["TERM", "INT"].each do |s|
          Signal.trap(s) do
            log(fn: "trap", signal: s, at: "exit", status: 0)
            Kernel.exit!(0)
          end
        end
      end
    end

    def self.start
      trap
      bind
      await
    end

    def self.log(data, &blk)
      Scrolls.log({ns: "receiver"}.merge(data), &blk)
    end
  end
end
