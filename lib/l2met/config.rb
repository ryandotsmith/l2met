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
      !env(key).nil?
    end

    def port; env!("PORT").to_i; end
    def l2met_consumer; env!("L2MET_CONSUMER"); end
    def librato_email; env!("LIBRATO_EMAIL"); end
    def librato_token; env!("LIBRATO_TOKEN"); end
    def dynamo?; env?("DYNAMO"); end
    def aws_id; env!("AWS_ID"); end
    def aws_secret; env!("AWS_SECRET"); end
    def num_dboutlets; env!("NUM_DBOUTLETS").to_i; end
  end
end
