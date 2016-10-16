module Clickhouse
  class Connection
    module Query
      class ResultSet
        include Enumerable
        extend Forwardable

        def_delegators :@rows, :size, :empty?
        def_delegators :to_a, :first, :last, :flatten

        def initialize(rows = [], names = nil, types = nil)
          @rows = rows
          @names = names
          @types = types
        end

        def each
          size.times{|i| yield self[i]}
        end

        def [](index)
          row = @rows[index]
          row = @rows[index] = parse_row(row) if row.class == Array
          row
        end

        def present?
          !empty?
        end

      private

        def parse_row(array)
          values = array.each_with_index.to_a.collect do |value, i|
            parse_value(@types[i], value) if @types
          end
          ResultRow.new values, @names
        end

        def parse_value(type, value)
          unless value == "NULL"
            case type
            when "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64"
              value.to_i
            when "Float32", "Float64"
              value.to_f
            when "String", "Enum8", "Enum16"
              value.force_encoding("UTF-8")
            when /FixedString\(\d+\)/
              value.delete("\000").force_encoding("UTF-8")
            when "Date"
              Date.parse(value)
            when "DateTime"
              Time.parse(value)
            when /Array\(/
              JSON.parse(value).flatten
            # when /Tuple\(/
            #   what to do?
            else
              raise NotImplementedError, "Cannot parse value of type #{type.inspect}"
            end
          end
        end

      end
    end
  end
end
