module Clickhouse
  class Connection
    module Client

      def self.included(base)
        base.extend Forwardable
        base.def_delegators :@client, :get
        base.def_delegators :@client, :post
      end

      def connect!
        return if connected?

        response = client.get "/"
        raise ConnectionError, "Unexpected response status: #{response.status}" unless response.status == 200
        true

      rescue Faraday::ConnectionFailed => e
        raise ConnectionError, e.message
      end

      def connected?
        instance_variables.include?(:@client) && !!@client
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
