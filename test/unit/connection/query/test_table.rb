require_relative "../../../test_helper"

module Unit
  module Connection
    module Query
      class TestTable < MiniTest::Test

        describe Clickhouse::Connection::Query::Table do
          it "generates a 'CREATE TABLE' statement" do
            table = Clickhouse::Connection::Query::Table.new("logs_test") do |t|
              t.uint8        :id
              t.float32      :price
              t.string       :name
              t.date         :date
              t.date_time    :time
              t.fixed_string :hex_id, 8
              t.engine       "MergeTree(date, 8192)"
            end

            sql = <<-SQL
CREATE TABLE logs_test (
  UInt8          id,
  Float32        price,
  String         name,
  Date           date,
  DateTime       time,
  FixedString(8) hex_id
)
ENGINE = MergeTree(date, 8192)
            SQL

            assert_equal sql.strip, table.to_sql.strip
          end
        end

      end
    end
  end
end
