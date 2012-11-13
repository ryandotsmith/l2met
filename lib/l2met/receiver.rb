require 'securerandom'
require 'scrolls'
require 'l2met/config'
require 'l2met/register'

module L2met
  module Receiver
    extend self

    LineRe = /^\d+ \<\d+\>1 (\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\+00:00) [a-z0-9-]+ ([a-z0-9\-\_\.]+) ([a-z0-9\-\_\.]+) \- (.*)$/
    IgnoreMsgRe = /(^ *$)|(Processing|Parameters|Completed|\[Worker\(host)/
    TimeSubRe = / \d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d-\d\d:\d\d/
    AttrsRe = /( *)([a-zA-Z0-9\_\-\.]+)=?(([a-zA-Z0-9\.\-\_\.]+)|("([^\"]+)"))?/

    def unpack(cid, s, beta=false)
      while s && s.length > 0
        if m = s.match(/^(\d+) /)
          num_bytes = m[1].to_i
          msg = s[m[0].length..(m[0].length + num_bytes)]
          if data = parse([m[0], msg.chomp].join)
            store_data(data.merge("consumer" => cid, "beta" => beta))
          end
          s = s[(m[0].length + num_bytes)..(s.length)]
        elsif m = s.match(/\n/)
          s = m.post_match
        else
          log(error: "unable to parse: #{s}")
        end
      end
    end

    def store_data(d)
      if d.key?("measure") and d["measure"].to_s != "true"
        return beta_store_data(d)
      end

      if d.key?("measure") && d.key?("app")
        Utils.count(1, ns: "receiver", at: "accept-measurement")
        opts = {source: d["app"], consumer: d["consumer"], time: d["time"]}
        if d.key?("fn") && d.key?("elapsed")
          name = [d["app"], d["fn"]].compact.join(".")
          Register.accept(name, Float(d["elapsed"]), opts.merge(type: 'list'))
          Register.accept(name, 1, opts.merge(type: 'counter'))
        end
        if d.key?("at") && !["start", "finish"].include?(d["at"])
          name = [d["app"], d["at"]].compact.join(".")
          if d.key?("last")
            Register.accept(name, Float(d["last"]), opts.merge(type: 'last'))
          else
            Register.accept(name, 1, opts.merge(type: 'counter'))
          end
        end
      end
    end

    def beta_store_data(d)
      if d.key?("measure")
        Utils.count(1, ns: "receiver", at: "accept-measurement")
        opts = {source: (d["app"] || 'default'), consumer: d["consumer"], time: d["time"]}
        name = [d["app"], d["measure"]].compact.join(".")
        Register.accept(name, 1, opts.merge(type: 'counter'))
        if d.key?("val")
          Register.accept(name, Float(d["val"]), opts.merge(type: 'list'))
        end
      end
    end

    def parse(line)
      if m = line.match(LineRe)
        if data = parse_msg(m[4])
          data["time"] = Time.parse(m[1])
          data["source"] = m[2]
          data["ps"] = m[3]
          data
        end
      end
    end

    def parse_msg(msg)
      if !msg.match(IgnoreMsgRe)
        data = {}
        #msg = msg.sub(TimeSubRe, "")
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

    def log(data, &blk)
      Scrolls.log({ns: "receiver"}.merge(data), &blk)
    end

  end
end
