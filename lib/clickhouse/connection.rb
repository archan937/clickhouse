module Clickhouse
  class Connection

    def initialize(config = {})
      @config = {
        :scheme => "http",
        :host => "localhost",
        :port => 8123
      }.merge(
        config.inject({}){|h, (k, v)| h[k.to_sym] = v; h}
      )
    end

    def ping!
      response = client.get "/"
      raise ConnectionError, "Unexpected response status: #{response.status}" unless response.status == 200
      true
    rescue Faraday::ConnectionFailed => e
      raise ConnectionError, e.message
    end

  private

    def url
      "#{@config[:scheme]}://#{@config[:host]}:#{@config[:port]}"
    end

    def client
      @client ||= Faraday.new(:url => url)
    end

  end
end
