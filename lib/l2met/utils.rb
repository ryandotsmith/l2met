require 'yajl'

module L2met
  module Utils
    extend self

    def enc_j(data)
      Yajl::Encoder.encode(data)
    end
  end
end
