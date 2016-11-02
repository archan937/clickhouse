require "clickhouse/connection/client"
require "clickhouse/connection/logger"
require "clickhouse/connection/query"

module Clickhouse
  class Connection

    DEFAULT_CONFIG = {
      :scheme => "http",
      :host => "localhost",
      :port => 8123
    }

    include Client
    include Logger
    include Query

    def initialize(config = {})
      @config = normalize_config(config)
    end

  private

    def normalize_config(config)
      config = config.inject({}) do |hash, (key, value)|
        hash[key.to_sym] = value
        hash
      end

      if url = config[:url]
        url = "#{DEFAULT_CONFIG[:scheme]}://#{url}" unless url.match(/^\w+:\/\//)
        uri = URI url
        config[:scheme] = uri.scheme
        config[:host] = uri.host
        config[:port] = uri.port
        config.delete(:url)
      end

      DEFAULT_CONFIG.merge(config.reject{|k, v| v.nil?})
    end

  end
end
