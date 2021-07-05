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
          @connection.stubs(:parse_stats)
          @connection.stubs(:write_log)
        end

        describe "#execute" do
          it "sends a POST request" do
            @connection.expects(:post).with("sql", nil).returns("")
            assert_equal true, @connection.execute("sql")
          end

          describe "when server returns a non-empty body" do
            it "returns the body of the response" do
              @connection.expects(:post).with("sql", "body").returns("Ok.")
              assert_equal "Ok.", @connection.execute("sql", "body")
            end
          end
        end

        describe "#query" do
          it "sends a GET request requesting a TSV response including names and types" do
            @connection.expects(:get).with("sql FORMAT JSONCompact")
            @connection.stubs(:parse_data)
            assert_equal [], @connection.query("sql").to_a
          end
        end

        describe "#databases" do
          it "sends a 'SHOW DATABASES' query" do
            @connection.expects(:get).with("SHOW DATABASES FORMAT JSONCompact")
            @connection.stubs(:parse_data).returns([])
            assert_equal [], @connection.databases
          end
        end

        describe "#tables" do
          it "sends a 'SHOW TABLES' query" do
            @connection.expects(:get).with("SHOW TABLES FORMAT JSONCompact")
            @connection.stubs(:parse_data).returns([])
            @connection.tables
          end
        end

        describe "#create_table" do
          it "sends a 'CREATE TABLE' query" do
            sql = <<-SQL
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
            @connection.expects(:post).with(sql.strip, nil).returns("")
            @connection.create_table("logs_test") do |t|
              t.uint8        :id
              t.float32      :price
              t.string       :name
              t.date         :date
              t.date_time    :time
              t.fixed_string :hex_id, 8
              t.engine       "MergeTree(date, 8192)"
            end
          end
        end

        describe "#describe_table" do
          it "sends a 'DESCRIBE TABLE <name>' query" do
            @connection.expects(:get).with("DESCRIBE TABLE logs FORMAT JSONCompact")
            @connection.stubs(:parse_data)
            @connection.describe_table("logs")
          end
        end

        describe "#rename_table" do
          describe "when passing an array with an even number of names" do
            it "sends a POST request containing a RENAME TABLE statement" do
              @connection.expects(:post).with("RENAME TABLE foo TO bar, baz TO qux", nil).returns("").twice
              assert_equal true, @connection.rename_table("foo", "bar", "baz", "qux")
              assert_equal true, @connection.rename_table(["foo", "bar"], ["baz", "qux"])
            end
          end

          describe "when passing an array with an odd number of names" do
            it "raises an Clickhouse::InvalidQueryError" do
              assert_raises Clickhouse::InvalidQueryError do
                @connection.rename_table "foo"
              end
              assert_raises Clickhouse::InvalidQueryError do
                @connection.rename_table ["foo"]
              end
            end
          end

          describe "when passing a hash" do
            it "sends a POST request containing a RENAME TABLE statement" do
              @connection.expects(:post).with("RENAME TABLE foo TO bar, baz TO qux", nil).returns("")
              assert_equal true, @connection.rename_table(:foo => "bar", :baz => "qux")
            end
          end
        end

        describe "#drop_table" do
          it "sends a POST request containing a 'DROP TABLE' statement" do
            @connection.expects(:post).with("DROP TABLE logs", nil).returns("")
            assert_equal true, @connection.drop_table("logs")
          end
        end

        describe "#exists_table" do
          it "sends a POST request containing a 'EXISTS TABLE' statement" do
            @connection.expects(:post).with("EXISTS TABLE logs", nil).returns("1")
            assert_equal true, @connection.exists_table("logs")
          end
        end

        describe "#insert_rows" do
          before do
            @jsonrows = <<-JSON
            {\"id\":12345,\"first_name\":\"Paul\",\"last_name\":\"Engel\"}
            {\"id\":67890,\"first_name\":\"Bruce\",\"last_name\":\"Wayne\"}
            JSON
            @jsonrows.gsub!(/^\s+/, "")
            @jsonrows.sub!(/\n+$/, "")
            # @csv = <<-CSV
            #   id,first_name,last_name
            #   12345,Paul,Engel
            #   67890,Bruce,Wayne
            # CSV
            # @csv.gsub!(/^\s+/, "")
          end

          describe "when using hashes" do
            it "sends a POST request containing a 'INSERT INTO' statement using JSONEachRow" do
              @connection.expects(:post).with("INSERT INTO logs FORMAT JSONEachRow", @jsonrows).returns("")
              assert_equal true, @connection.insert_rows("logs") { |rows|
                rows << {:id => 12345, :first_name => "Paul", :last_name => "Engel"}
                rows << {:id => 67890, :first_name => "Bruce", :last_name => "Wayne"}
              }
            end
          end

          describe "when using arrays" do
            it "sends a POST request containing a 'INSERT INTO' statement using CSV" do
              @connection.expects(:post).with("INSERT INTO logs FORMAT JSONEachRow", @jsonrows).returns("")
              assert_equal true, @connection.insert_rows("logs", :names => %w(id first_name last_name)) { |rows|
                rows << [12345, "Paul", "Engel"]
                rows << [67890, "Bruce", "Wayne"]
              }
            end
          end
        end

        describe "#select_rows" do
          it "sends a GET request and parses the result set" do
            body = <<-JAVASCRIPT
              {
                "meta": [
                  {"name": "year", "type": "UInt16"},
                  {"name": "name", "type": "String"}
                ],
                "data": [
                  [1982, "Paul"],
                  [1947, "Anna"]
                ]
              }
            JAVASCRIPT

            @connection.expects(:to_select_query).with(options = {:from => "logs"}).returns("")
            @connection.expects(:get).returns(JSON.parse(body))

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
              @connection.expects(:to_select_query).returns("")
              @connection.expects(:get).returns(stub(:body => ""))
              @connection.stubs(:parse_data).returns([])
              assert_equal [], @connection.select_values({})
            end
          end

          describe "when getting data" do
            it "returns every first value of every row" do
              body = <<-JAVASCRIPT
                {
                  "meta": [
                    {"name": "year", "type": "UInt16"},
                    {"name": "name", "type": "String"}
                  ],
                  "data": [
                    [1982, "Paul"],
                    [1947, "Anna"]
                  ]
                }
              JAVASCRIPT

              @connection.expects(:to_select_query).returns("")
              @connection.expects(:get).returns(JSON.parse(body))
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

        describe "#count" do
          it "returns the first value of the first row" do
            @connection.expects(:select_value).with(:select => "COUNT(*)", :from => "logs").returns(1982)
            assert_equal 1982, @connection.count(:from => "logs")
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
