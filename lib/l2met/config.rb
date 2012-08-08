module L2met
  module Config
    extend self

    def env(key)
      ENV[key]
    end

    def env!(key)
      env(key) || raise("missing #{key}")
    end

    def port; env!("PORT").to_i; end
    def librato_email; env!("LIBRATO_EMAIL"); end
    def librato_token; env!("LIBRATO_TOKEN"); end

  end
end
