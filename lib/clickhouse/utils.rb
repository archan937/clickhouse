module Clickhouse
  module Utils
    extend self

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
