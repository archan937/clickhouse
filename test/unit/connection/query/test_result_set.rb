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
                  [4, 2, 5, 7]
                ], [
                  "12948140",
                  "9320.11",
                  "engel.pm",
                  "d91d217c\u0000\u0000",
                  "2016-03-20",
                  "2016-03-20 23:58:34",
                  [6, 2, 9, 8, 1]
                ], [
                  "319384",
                  "101.02",
                  "archan937.com",
                  "d91d2294\u0000\u0000\u0000",
                  "2016-03-20",
                  "2016-03-20 22:55:39",
                  [3, 1, 2]
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

          describe "#to_hashes" do
            it "returns an array containing the rows as hashes" do
              assert_equal [
                {
                  "SUM(clicks)" => 1072649,
                  "AVG(price)" => 142.94,
                  "domain" => "badrequest.io",
                  "id" => "d91d1c90",
                  "date" => Date.new(2016, 3, 20),
                  "MAX(time)" => Time.new(2016, 3, 20, 23, 49, 11),
                  "groupUniqArray(code)" => [4, 2, 5, 7]
                }, {
                  "SUM(clicks)" => 12948140,
                  "AVG(price)" => 9320.11,
                  "domain" => "engel.pm",
                  "id" => "d91d217c",
                  "date" => Date.new(2016, 3, 20),
                  "MAX(time)" => Time.new(2016, 3, 20, 23, 58, 34),
                  "groupUniqArray(code)" => [6, 2, 9, 8, 1]
                }, {
                  "SUM(clicks)" => 319384,
                  "AVG(price)" => 101.02,
                  "domain" => "archan937.com",
                  "id" => "d91d2294",
                  "date" => Date.new(2016, 3, 20),
                  "MAX(time)" => Time.new(2016, 3, 20, 22, 55, 39),
                  "groupUniqArray(code)" => [3, 1, 2]
                }
              ], @result_set.to_hashes
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

          describe 'Nullable Integer' do
            before do
              @result_set = Clickhouse::Connection::Query::ResultSet.new(
                  [
                      [nil, nil, nil, nil, nil, nil, nil],
                      [1, 1, 1, 1, 1, 1, 1]
                  ],
                  [
                      "column_uint8",
                      "column_uint16",
                      "column_uint32",
                      "column_uint64",
                      "column_int8",
                      "column_int16",
                      "column_int32",
                      "column_int64"
                  ],
                  [
                      "Nullable(UInt8)",
                      "Nullable(UInt16)",
                      "Nullable(UInt32)",
                      "Nullable(UInt64)",
                      "Nullable(Int8)",
                      "Nullable(Int16)",
                      "Nullable(Int32)",
                      "Nullable(Int64)"
                  ]
              )
            end

            it 'should parse as Integer' do
              assert_equal [[0, 0, 0, 0, 0, 0, 0], [1, 1, 1, 1, 1, 1, 1]],
                           @result_set.to_a
            end
          end

          describe 'Nullable Float' do
            before do
              @result_set = Clickhouse::Connection::Query::ResultSet.new(
                  [
                      [nil, nil],
                      [1, 1]
                  ],
                  [
                      "column_nullable_float64",
                      "column_nullable_float32"
                  ],
                  [
                      "Nullable(Float64)",
                      "Nullable(Float32)"
                  ]
              )
            end

            it 'should parse as Float' do
              assert_equal ["0.0", "0.0", "1.0", "1.0"],
                           @result_set.flatten.map(&:inspect)
            end
          end

          describe 'Nullable String' do
            before do
              @result_set = Clickhouse::Connection::Query::ResultSet.new(
                  [
                      [nil, nil],
                      ['', ''],
                      ['text', 'T']
                  ],
                  [
                      "column_nullable_string",
                      "column_fixed_string_1"
                  ],
                  [
                      "Nullable(String)",
                      "Nullable(FixedString(1))"
                  ]
              )
            end

            it 'should parse as String' do
              assert_equal [["", ""], ["", ""], ["text", "T"]],
                           @result_set.to_a
            end
          end

          describe 'Nullable DateTime' do
            before do
              @result_set = Clickhouse::Connection::Query::ResultSet.new(
                  [
                      [nil, nil],
                      ["2016-03-20", "2016-03-20 22:55:39"]
                  ],
                  [
                      "column_nullable_date",
                      "column_date_time"
                  ],
                  [
                      "Nullable(Date)",
                      "Nullable(DateTime)"
                  ]
              )
            end

            it 'should parse as Date/DateTime' do
              assert_equal [[nil, nil], [Date.new(2016, 3, 20), Time.new(2016, 3, 20, 22, 55, 39)]],
                           @result_set.to_a
            end
          end
        end

      end
    end
  end
end
