require "clickhouse/connection/client"
require "clickhouse/connection/logger"
require "clickhouse/connection/query"

module Clickhouse
  class Connection

    include Client
    include Logger
    include Query

    def initialize(config = {})
      @config = {
        :scheme => "http",
        :host => "localhost",
        :port => 8123
      }.merge(
        config.inject({}){|h, (k, v)| h[k.to_sym] = v; h}
      )
    end

  end
end
