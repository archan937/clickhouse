module Clickhouse
  class Connection
    module Client
      include ActiveSupport::NumberHelper

      def connect!
        ping! unless connected?
      end

      def ping!
        ensure_authentication
        status = client.post("/", 'SELECT 1 = 1').status
        if status != 200
          raise ConnectionError, "Unexpected response status: #{status}"
        end
        true
      rescue Faraday::Error => e
        raise ConnectionError, e.message
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

      def url
        "#{@config[:scheme]}://#{@config[:host]}:#{@config[:port]}"
      end

    private

      def client
        @client ||= Faraday.new(:url => url)
      end

      def ensure_authentication
        username, password = @config.values_at(:username, :password)
        client.basic_auth(username || "default", password) if username || password
      end

      def path(query)
        params = @config.select{|k, _v| k == :database}
        params[:query] = query
        params[:output_format_write_statistics] = 1
        query_string = params.collect{|k, v| "#{k}=#{CGI.escape(v.to_s)}"}.join("&")

        "/?#{query_string}"
      end

      def request(method, query, body = nil)
        connect!
        query = query.strip
        start = Time.now

        response = client.send(method, path(query), body)
        status = response.status
        duration = Time.now - start
        query, format = Utils.extract_format(query)
        response = parse_body(format, response.body)
        stats = parse_stats(response)

        write_log duration, query, stats
        raise QueryError, "Got status #{status} (expected 200): #{response}" unless status == 200
        response

      rescue Faraday::Error => e
        raise ConnectionError, e.message
      end

      def parse_body(format, body)
        case format
        when "JSON", "JSONCompact"
          body = JSON.parse(body) if body.strip[0] == "{"
        end
        body
      end

      def parse_stats(response)
        return {} unless response.is_a?(Hash)

        options = {:locale => :en, :precision => 2, :significant => false}
        stats = response["statistics"].merge("rows" => response["rows"])
        factor = 1 / stats["elapsed"]

        stats["elapsed"] = number_to_human_duration(stats["elapsed"])
        stats["rows_per_second"] = number_to_human(stats["rows_read"] * factor, options).downcase
        stats["data_per_second"] = number_to_human_size(stats["bytes_read"] * factor, options)
        stats["rows_read"] = number_to_human(stats["rows_read"], options).downcase
        stats["data_read"] = number_to_human_size(stats["bytes_read"], options)

        stats
      end

      def write_log(duration, query, stats)
        duration = number_to_human_duration(duration)

        rows,
        elapsed,
        rows_read,
        data_read,
        rows_per_second,
        data_per_second = stats.values_at(*%w(
          rows
          elapsed
          rows_read
          data_read
          rows_per_second
          data_per_second
        ))

        line1 = "\n \e[1m[35mSQL (#{duration})\e[0m  #{query};"
        line2 = "\n  \e[1m[36m#{rows} #{"row".pluralize(rows)} in set. Elapsed: #{elapsed}. Processed: #{rows_read} rows, #{data_read} (#{rows_per_second} rows/s, #{data_per_second}/s)\e[0m" if rows

        log :debug, "#{line1}#{line2} "
      end

      def number_to_human_duration(number)
        if (amount = number * 1000.0) < 1000
          round = (amount < 1) ? 3 : 1
          "#{amount.round(round)}ms"
        else
          "#{number.round(1)}s"
        end
      end

    end
  end
end
