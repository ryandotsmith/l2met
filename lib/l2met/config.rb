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

    def app_name; env!("APP_NAME"); end
    def app_url; env!("APP_URL"); end
    def port; env!("PORT").to_i; end
    def l2met_consumer; env!("L2MET_CONSUMER"); end
    def aws_id; env!("AWS_ID"); end
    def aws_secret; env!("AWS_SECRET"); end
    def num_outlets; env!("NUM_OUTLETS").to_i; end
    def redis_url; env!("REDISGREEN_URL"); end
  end
end
