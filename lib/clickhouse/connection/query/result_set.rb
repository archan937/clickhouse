module Clickhouse
  class Connection
    module Query
      class ResultSet
        include Enumerable
        extend Forwardable

        def_delegators :@rows, :size, :empty?
        def_delegators :to_a, :first, :last, :flatten

        attr_reader :names, :types

        def initialize(rows = [], names = nil, types = nil)
          @rows = rows
          @names = names
          @types = types
        end

        def each
          (0..(size - 1)).collect do |index|
            yield self[index]
            self[index]
          end
        end

        def [](index)
          row = @rows[index]
          row = @rows[index] = parse_row(row) if row.class == Array
          row
        end

        def present?
          !empty?
        end

        def to_hashes(symbolize = false)
          collect{|row| row.to_hash(symbolize)}
        end

      private

        def parse_row(array)
          values = array.each_with_index.to_a.collect do |value, i|
            parse_value(@types[i], value) if @types
          end
          ResultRow.new values, @names
        end

        def parse_value(type, value)
          case type
          when "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64"
            parse_int_value value
          when "Nullable(UInt8)", "Nullable(UInt16)", "Nullable(UInt32)", "Nullable(UInt64)", "Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)",  "Nullable(Int64)"
            parse_int_value value
          when "Float32", "Float64", "Nullable(Float64)", "Nullable(Float32)"
            parse_float_value value
          when "String", "Enum8", "Enum16", "Nullable(String)"
            parse_string_value value
          when /FixedString\(\d+\)/
            parse_fixed_string_value value
          when "Date", "Nullable(Date)"
            parse_date_value value
          when "DateTime", "Nullable(DateTime)"
            parse_date_time_value value
          when /Array\(/
            parse_array_value value
          else
            raise NotImplementedError, "Cannot parse value of type #{type.inspect}"
          end
        end

        def parse_int_value(value)
          value.to_i
        end

        def parse_float_value(value)
          value.to_f
        end

        def parse_string_value(value)
          value.to_s.force_encoding("UTF-8")
        end

        def parse_fixed_string_value(value)
          value.to_s.delete("\000").force_encoding("UTF-8")
        end

        def parse_date_value(value)
          Date.parse(value) rescue nil # "rescue nil" for Nullable
        end

        def parse_date_time_value(value)
          Time.parse(value) rescue nil # "rescue nil" for Nullable
        end

        def parse_array_value(value)
          value
        end

      end
    end
  end
end
