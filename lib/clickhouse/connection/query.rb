require "clickhouse/connection/query/result_set"
require "clickhouse/connection/query/result_row"

module Clickhouse
  class Connection
    module Query

      def select_rows(options)
        connect!

        start = Time.now
        query = to_select_query(options)

        response = get("/", {:query => query})
        log :info, "\n  [1m[35mSQL (#{((Time.now - start) * 1000).round(1)}ms)[0m  #{query}[0m"

        parse_response response.body
      end

      def select_row(options)
        select_rows(options)[0]
      end

      def select_values(options)
        select_rows(options).collect{|row| row[0]}
      end

      def select_value(options)
        values = select_values(options)
        values[0] if values
      end

    private

      def inspect_value(value)
        value.nil? ? "NULL" : value.inspect.gsub(/(^"|"$)/, "'").gsub("\\\"", "\"")
      end

      def to_select_options(options)
        keys = [:select, :from, :where, :group, :having, :order, :limit, :offset, :format]

        options = Hash[keys.zip(options.values_at(*keys))]
        options[:select] ||= "*"
        options[:limit] ||= 0 if options[:offset]
        options[:limit] = options.values_at(:offset, :limit).compact.join(", ") if options[:limit]
        options[:format] = "TabSeparatedWithNamesAndTypes"
        options.delete(:offset)

        options
      end

      def to_segment(type, value)
        case type
        when :select
          [value].flatten.join(", ")
        when :where, :having
          to_condition_statements(value)
        else
          value
        end
      end

      def to_condition_statements(value)
        value.collect do |attr, val|
          if val == :empty
            "empty(#{attr})"
          elsif val.is_a?(Range)
            [
              "#{attr} >= #{inspect_value(val.first)}",
              "#{attr} <= #{inspect_value(val.last)}"
            ]
          elsif val.is_a?(Array)
            "#{attr} IN (#{val.collect{|x| inspect_value(x)}.join(", ")})"
          elsif val.to_s.match(/^`.*`$/)
            "#{attr} #{val.gsub(/(^`|`$)/, "")}"
          else
            "#{attr} = #{inspect_value(val)}"
          end
        end.flatten.join(" AND ")
      end

      def to_select_query(options)
        to_select_options(options).collect do |(key, value)|
          next if value.nil? && (!value.respond_to?(:empty?) || value.empty?)

          statement = [key.to_s.upcase]
          statement << "BY" if %W(GROUP ORDER).include?(statement[0])
          statement << to_segment(key, value)
          statement.join(" ")

        end.compact.join("\n").force_encoding("UTF-8")
      end

      def parse_response(response)
        rows = CSV.parse response.to_s, :col_sep => "\t"
        names = rows.shift
        types = rows.shift
        ResultSet.new rows, names, types
      end

    end
  end
end
