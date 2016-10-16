module Clickhouse
  class Connection
    module Query
      class ResultRow < Array

        def initialize(values = [], keys = nil)
          super values
          @keys = keys || (0..(values.size - 1)).collect{|i| "column#{i}"}
        end

        def to_hash
          @hash ||= Hash[@keys.zip(self)]
        end

      end
    end
  end
end
