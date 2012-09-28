require 'l2met/db'
require 'securerandom'

module L2met
  module Consumer
    extend self

    def all(user)
      DB["consumers"].select.select do |item|
        item.attributes["created_by"] == user
      end.map {|i| api_vals(i.attributes)}
    end

    def put(user, data)
      if data[:id].to_s.length.zero?
        data[:id] = SecureRandom.uuid
      end
      d = {id: data[:id],
        email: data[:email],
        token: data[:token],
        created_by: user}
      c = DB["consumers"].create(d)
      api_vals(c.attributes.to_h)
    end

    def get(id)
      c = DB["consumers"].at(id)
      api_vals(c.attributes.to_h)
    end

    def api_vals(data)
      {id: data['id'],
        drain_url: [Config.app_url, 'consumers', data['id'], 'logs'].join('/'),
        email: data["email"],
        token: data["token"]}
    end
  end
end
