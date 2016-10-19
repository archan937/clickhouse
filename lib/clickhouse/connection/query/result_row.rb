module Clickhouse
  class Connection
    module Query
      class ResultRow < Array

        def initialize(values = [], keys = nil)
          super values
          @keys = normalize_keys(keys)
        end

        def to_hash(symbolize = false)
          @hash ||= begin
            keys = symbolize ? @keys.collect(&:to_sym) : @keys
            Hash[keys.zip(self)]
          end
        end

      private

        def normalize_keys(keys)
          if keys
            keys.collect do |key|
              key.match(/^any\(([^\)]+)\)$/)
              $1 || key
            end
          else
            (0..(size - 1)).collect do |index|
              "column#{index}"
            end
          end
        end

      end
    end
  end
end
