module Clickhouse
  class Connection
    module Client

      def connect!
        return if connected?
        ensure_authentication
        ping!
      end

      def connected?
        instance_variables.include?(:@client) && !!@client
      end

      def get(query)
        request(:get, query)
      end

      def post(query, body = nil)
        request(:post, query, body)
      end

    private

      def url
        "#{@config[:scheme]}://#{@config[:host]}:#{@config[:port]}"
      end

      def path(query)
        database = "database=#{@config[:database]}&" if @config[:database]
        "/?#{database}query=#{CGI.escape(query)}"
      end

      def client
        @client ||= Faraday.new(:url => url)
      end

      def ensure_authentication
        username, password = @config.values_at(:username, :password)
        client.basic_auth(username || "default", password) if username || password
      end

      def ping!
        status = client.get("/").status
        if status != 200
          raise ConnectionError, "Unexpected response status: #{status}"
        end
        true
      rescue Faraday::ConnectionFailed => e
        raise ConnectionError, e.message
      end

      def request(method, query, body = nil)
        connect!
        query = query.to_s.strip
        start = Time.now
        client.send(method, path(query), body).tap do |response|
          log :info, "\n  [1m[35mSQL (#{((Time.now - start) * 1000).round(1)}ms)[0m  #{query.gsub(/( FORMAT \w+|;$)/, "")};[0m"
          raise QueryError, response.body unless response.status == 200
        end
      end

    end
  end
end
