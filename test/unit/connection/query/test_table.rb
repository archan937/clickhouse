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

            sql = <<~SQL
              CREATE TABLE logs_test (
                id     UInt8,
                price  Float32,
                name   String,
                date   Date,
                time   DateTime,
                hex_id FixedString(8)
              )
              ENGINE = MergeTree(date, 8192)
            SQL

            assert_equal sql.strip, table.to_sql.strip
          end

          it "generates a 'CREATE TABLE IF NOT EXISTS' statement" do
            table = Clickhouse::Connection::Query::Table.new("logs_test") do |t|
              t.if_not_exists
              t.uint8         :id
              t.float32       :price
              t.string        :name
              t.date          :date
              t.date_time     :time
              t.fixed_string  :hex_id, 8
              t.engine        "MergeTree(date, 8192)"
            end

            sql = <<~SQL
              CREATE TABLE IF NOT EXISTS logs_test (
                id     UInt8,
                price  Float32,
                name   String,
                date   Date,
                time   DateTime,
                hex_id FixedString(8)
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
