module Clickhouse
  class Connection
    module Query
      class Table

        def initialize(name)
          @name = name
          @columns = []
          yield self
        end

        def engine(value)
          @engine = value
        end

        def to_sql
          raise Clickhouse::InvalidQueryError, "Missing table engine" unless @engine
          length = @columns.collect{|x| x[0].to_s.size}.max

          sql = []
          sql << "CREATE TABLE #{@name} ("

          @columns.each_with_index do |(name, type), index|
            sql << "  #{name.ljust(length, " ")} #{type}#{"," unless index == @columns.size - 1}"
          end

          sql << ")"
          sql << "ENGINE = #{@engine}"

          sql.join("\n")
        end

      private

        def method_missing(name, *args)
          type = name.to_s
                  .gsub(/(^.|_\w)/) {
                    $1.upcase
                  }
                  .gsub("Uint", "UInt")
                  .delete("_")

          type << "(#{args[1]})" if args[1]
          @columns << [type, args[0]]
        end

      end
    end
  end
end
