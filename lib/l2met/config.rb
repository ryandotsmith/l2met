module L2met
  module Config
    extend self

    def env(key)
      ENV[key]
    end

    def env!(key)
      env(key) || raise("missing #{key}")
    end

    def env?(key)
      env(key) || false
    end

    def port; env!("PORT").to_i; end
    def librato_email; env!("LIBRATO_EMAIL"); end
    def librato_token; env!("LIBRATO_TOKEN"); end
    def dynamo?; env?("DYNAMO"); end
    def aws_id; env!("AWS_ID"); end
    def aws_secret; env!("AWS_SECRET"); end
  end
end
