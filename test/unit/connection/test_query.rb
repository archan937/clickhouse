require_relative "../../test_helper"

module Unit
  module Connection
    class TestQuery < MiniTest::Test

      class Connection < SimpleConnection
        include Clickhouse::Connection::Query
        include Clickhouse::Connection::Logger
      end

      describe Clickhouse::Connection::Query do
        before do
          @connection = Connection.new
        end

        describe "#select_rows" do
          it "queries and parses the result set" do
            body = <<-TSV
              year\tname
              UInt16\tString
              1982\tPaul
              1947\tAnna
            TSV

            @connection.expects(:to_select_query).with(options = {:from => "logs"})
            @connection.expects(:request).returns(stub({body: body.gsub(/^\s+/, "")}))
            assert_equal [
              [1982, "Paul"],
              [1947, "Anna"]
            ], @connection.select_rows(options).to_a
          end
        end

        describe "#select_row" do
          it "returns an empty array" do
            @connection.expects(:select_rows).returns([["Paul", "Engel"], ["Bruce", "Wayne"]])
            assert_equal ["Paul", "Engel"], @connection.select_row({})
          end
        end

        describe "#select_values" do
          describe "when empty result set" do
            it "returns an empty array" do
              @connection.expects(:to_select_query)
              @connection.expects(:request).returns(stub({body: ""}))
              assert_equal [], @connection.select_values({})
            end
          end

          describe "when getting data" do
            it "returns every first value of every row" do
              body = <<-TSV
                year\tname
                UInt16\tString
                1982\tPaul
                1947\tAnna
              TSV

              @connection.expects(:to_select_query)
              @connection.expects(:request).returns(stub({body: body.gsub(/^\s+/, "")}))
              assert_equal [
                1982,
                1947
              ], @connection.select_values({})
            end
          end
        end

        describe "#select_value" do
          describe "when empty result set" do
            it "returns nil" do
              @connection.expects(:select_values).with(options = {:foo => "bar"}).returns([])
              assert_nil @connection.select_value(options)
            end
          end

          describe "when getting data" do
            it "returns the first value of the first row" do
              @connection.expects(:select_values).with(options = {:foo => "bar"}).returns([1982])
              assert_equal 1982, @connection.select_value(options)
            end
          end
        end

        describe "#to_select_query" do
          describe "when passing :from option" do
            it "generates a simple 'SELECT * FROM <table>' query" do
              query = <<-SQL
                SELECT *
                FROM logs
              SQL
              options = {
                :from => "logs"
              }
              assert_query(query, @connection.send(:to_select_query, options))
            end
          end

          describe "when passing :from and :select option" do
            describe "when passing a single column" do
              it "respects the single column in the SELECT statement" do
                query = <<-SQL
                  SELECT MIN(date)
                  FROM logs
                SQL
                options = {
                  :select => "MIN(date)",
                  :from => "logs"
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end

            describe "when passing multiple columns" do
              it "only includes the passed columns in the SELECT statement" do
                query = <<-SQL
                  SELECT MIN(date), MAX(date)
                  FROM logs
                SQL
                options = {
                  :select => ["MIN(date)", "MAX(date)"],
                  :from => "logs"
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end

            describe "when filtering on value is empty" do
              it "uses the empty() function in the WHERE statement" do
                query = <<-SQL
                  SELECT *
                  FROM logs
                  WHERE empty(parent_id)
                SQL
                options = {
                  :from => "logs",
                  :where => {
                    :parent_id => :empty
                  }
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end

            describe "when filtering on value is within a certain range" do
              it "includes the range in the WHERE statement" do
                query = <<-SQL
                  SELECT *
                  FROM logs
                  WHERE code >= 6 AND code <= 10
                SQL
                options = {
                  :from => "logs",
                  :where => {
                    :code => 6..10
                  }
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end

            describe "when filtering on value in array" do
              it "uses an IN operator in the WHERE statement" do
                query = <<-SQL
                  SELECT *
                  FROM logs
                  WHERE code IN (6, 7, 8, 9, 10)
                SQL
                options = {
                  :from => "logs",
                  :where => {
                    :code => [6, 7, 8, 9, 10]
                  }
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end

            describe "when filtering using backticks" do
              it "uses the specified SQL as is" do
                query = <<-SQL
                  SELECT *
                  FROM logs
                  WHERE id != 'cb5a67d2932911e6'
                SQL
                options = {
                  :from => "logs",
                  :where => {
                    :id => "`!= 'cb5a67d2932911e6'`"
                  }
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end

            describe "when filtering on a string" do
              it "uses a single quoted string" do
                query = <<-SQL
                  SELECT *
                  FROM logs
                  WHERE id = 'cb5a67d2932911e6'
                SQL
                options = {
                  :from => "logs",
                  :where => {
                    :id => "cb5a67d2932911e6"
                  }
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end

            describe "when using all options" do
              it "generates the complex query" do
                query = <<-SQL
                  SELECT date, COUNT(id), groupUniqArray(severity), SUM(clicks)
                  FROM logs
                  WHERE date >= '2016-08-01' AND hidden = 0
                  GROUP BY date
                  HAVING MIN(severity) = 2
                  ORDER BY MIN(time) DESC
                  LIMIT 120, 60
                SQL
                options = {
                  :select => ["date", "COUNT(id)", "groupUniqArray(severity)", "SUM(clicks)"],
                  :from => "logs",
                  :where => {
                    :date => "`>= '2016-08-01'`",
                    :hidden => 0
                  },
                  :group => "date",
                  :having => {
                    "MIN(severity)" => 2
                  },
                  :order => "MIN(time) DESC",
                  :limit => 60,
                  :offset => 120
                }
                assert_query(query, @connection.send(:to_select_query, options))
              end
            end
          end
        end
      end

    end
  end
end
