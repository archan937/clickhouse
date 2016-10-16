module Clickhouse
  class Connection
    module Client

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

      def get(query)
        request(:get, query).body
      end

      def post(query, body = nil)
        request(:post, query, body)
      end

    private

      def url
        "#{@config[:scheme]}://#{@config[:host]}:#{@config[:port]}"
      end

      def client
        @client ||= Faraday.new(:url => url)
      end

      def request(method, query, body = nil)
        connect!
        query = query.to_s.strip
        start = Time.now
        client.send(method, "/?query=#{CGI.escape(query)}", body).tap do |response|
          log :info, "\n  [1m[35mSQL (#{((Time.now - start) * 1000).round(1)}ms)[0m  #{query.gsub(/( FORMAT \w+|;$)/, "")};[0m"
          raise QueryError, response.body unless response.status == 200
        end
      end

    end
  end
end
