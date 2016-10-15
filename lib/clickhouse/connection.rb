require "clickhouse/connection/client"

module Clickhouse
  class Connection

    include Client

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
