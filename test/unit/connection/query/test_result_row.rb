require_relative "../../../test_helper"

module Unit
  module Connection
    module Query
      class TestResultRow < MiniTest::Test

        describe Clickhouse::Connection::Query::ResultRow do
          describe "#to_hash" do
            describe "when passing names" do
              it "uses the names as hash keys" do
                result_row = Clickhouse::Connection::Query::ResultRow.new([1, 2, 3], [:a, :b, :c])
                assert_equal({:a => 1, :b => 2, :c => 3}, result_row.to_hash)
              end
            end

            describe "when not passing names" do
              it "uses 'column<i>' as hash keys" do
                result_row = Clickhouse::Connection::Query::ResultRow.new([1, 2, 3])
                assert_equal({"column0" => 1, "column1" => 2, "column2" => 3}, result_row.to_hash)
              end
            end

            describe "memoization" do
              it "memoizes the resulting hash" do
                result_row = Clickhouse::Connection::Query::ResultRow.new([1, 2, 3])
                assert_equal result_row.to_hash.object_id, result_row.to_hash.object_id
              end
            end
          end
        end

      end
    end
  end
end
