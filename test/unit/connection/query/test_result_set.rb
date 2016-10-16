require_relative "../../../test_helper"

module Unit
  module Connection
    module Query
      class TestResultSet < MiniTest::Test

        describe Clickhouse::Connection::Query::ResultSet do
          before do
            @empty_set = Clickhouse::Connection::Query::ResultSet.new
            @result_set = Clickhouse::Connection::Query::ResultSet.new(
              [
                [
                  "1072649",
                  "142.94",
                  "badrequest.io",
                  "d91d1c90\u0000\u0000\u0000",
                  "2016-03-20",
                  "2016-03-20 23:49:11",
                  "[4,2,5,7]"
                ], [
                  "12948140",
                  "9320.11",
                  "engel.pm",
                  "d91d217c\u0000\u0000",
                  "2016-03-20",
                  "2016-03-20 23:58:34",
                  "[6,2,9,8,1]"
                ], [
                  "319384",
                  "101.02",
                  "archan937.com",
                  "d91d2294\u0000\u0000\u0000",
                  "2016-03-20",
                  "2016-03-20 22:55:39",
                  "[3,1,2]"
                ]
              ],
              %w(
                SUM(clicks)
                AVG(price)
                domain
                id
                date
                MAX(time)
                groupUniqArray(code)
              ),
              %w(
                UInt32
                Float32
                String
                FixedString(16)
                Date
                DateTime
                Array(8)
              )
            )
          end

          describe "#size" do
            it "returns the size of the result set" do
              assert_equal 3, @result_set.size
            end
          end

          describe "#empty?" do
            it "returns whether the result set is empty or not" do
              assert_equal true, @empty_set.empty?
              assert_equal false, @result_set.empty?
            end
          end

          describe "#present?" do
            it "returns whether the result set contains rows or not" do
              assert_equal false, @empty_set.present?
              assert_equal true, @result_set.present?
            end
          end

          describe "#first" do
            it "returns the first row of the result set" do
              assert_equal [
                1072649,
                142.94,
                "badrequest.io",
                "d91d1c90",
                Date.new(2016, 3, 20),
                Time.new(2016, 3, 20, 23, 49, 11),
                [4, 2, 5, 7]
              ], @result_set.first
            end
          end

          describe "#last" do
            it "returns the size of the result set" do
              assert_equal [
                319384,
                101.02,
                "archan937.com",
                "d91d2294",
                Date.new(2016, 3, 20),
                Time.new(2016, 3, 20, 22, 55, 39),
                [3, 1, 2]
              ], @result_set.last
            end
          end

          describe "#flatten" do
            it "returns the size of the result set" do
              assert_equal [
                1072649,
                142.94,
                "badrequest.io",
                "d91d1c90",
                Date.new(2016, 3, 20),
                Time.new(2016, 3, 20, 23, 49, 11),
                4,
                2,
                5,
                7,
                12948140,
                9320.11,
                "engel.pm",
                "d91d217c",
                Date.new(2016, 3, 20),
                Time.new(2016, 3, 20, 23, 58, 34),
                6,
                2,
                9,
                8,
                1,
                319384,
                101.02,
                "archan937.com",
                "d91d2294",
                Date.new(2016, 3, 20),
                Time.new(2016, 3, 20, 22, 55, 39),
                3,
                1,
                2
              ], @result_set.flatten
            end
          end

          describe "memoization" do
            it "memoizes the parsed rows" do
              assert_equal @result_set.to_a[-1].object_id, @result_set.each{}[-1].object_id
              assert_equal @result_set.first.object_id, @result_set[0].object_id
            end
          end

          describe "non-supported data types" do
            it "raises a NotImplementedError error" do
              assert_raises NotImplementedError do
                Clickhouse::Connection::Query::ResultSet.new([[1]], ["Foo"], ["Bar"])[0]
              end
            end
          end
        end

      end
    end
  end
end
