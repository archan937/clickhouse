module Clickhouse
  class Connection
    module Client

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
end
