module Clickhouse
  module Utils
    extend self

    def normalize_url(url)
      if url.match(/^\w+:\/\//)
        url
      else
        "#{Clickhouse::Connection::DEFAULT_CONFIG[:scheme]}://#{url}"
      end
    end

    def extract_format(query)
      format = nil
      query = query.gsub(/ FORMAT (\w+)/i) do
        format = $1
        ""
      end
      [query.strip, format]
    end

  end
end
