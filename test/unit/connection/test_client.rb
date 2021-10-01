require_relative "../../test_helper"

module Unit
  module Connection
    class TestClient < MiniTest::Test

      class Connection < SimpleConnection
        include Clickhouse::Connection::Client
      end

      describe Clickhouse::Connection::Client do
        before do
          @connection = Connection.new
          @connection.stubs(:parse_stats)
          @connection.stubs(:write_log)
        end

        describe "#connect!" do
          describe "when failed to connect" do
            it "returns true" do
              Faraday::Connection.any_instance.expects(:post).raises(Faraday::ConnectionFailed.new("Failed to connect"))
              assert_raises Clickhouse::ConnectionError do
                @connection.connect!
              end
            end
          end

          describe "when receiving 200" do
            it "returns true" do
              Faraday::Connection.any_instance.expects(:post).returns(stub(:status => 200))
              assert_equal true, @connection.connect!
            end
          end

          describe "when receiving 500" do
            it "raises a Clickhouse::ConnectionError" do
              Faraday::Connection.any_instance.expects(:post).returns(stub(:status => 500))
              assert_raises Clickhouse::ConnectionError do
                @connection.connect!
              end
            end
          end

          describe "when already connected" do
            it "returns nil" do
              @connection.instance_variable_set :@client, mock
              assert_nil @connection.connect!
            end
          end
        end

        describe "#connected?" do
          it "returns whether it has an connected socket" do
            assert_equal false, @connection.connected?
            @connection.instance_variable_set :@client, mock
            assert_equal true, @connection.connected?
            @connection.instance_variable_set :@client, nil
            assert_equal false, @connection.connected?
          end
        end

        describe "#get" do
          it "sends a GET request the server" do
            @connection.instance_variable_set :@client, (client = mock)
            client.expects(:get).with("/?query=foo&output_format_write_statistics=1", nil).returns(stub(:status => 200, :body => ""))
            @connection.stubs(:log)
            @connection.get("foo")
          end
        end

        describe "#post" do
          it "sends a POST request the server" do
            @connection.instance_variable_set :@client, (client = mock)
            client.expects(:post).with("/?query=foo&output_format_write_statistics=1", "body").returns(stub(:status => 200, :body => ""))
            @connection.stubs(:log)
            @connection.post("foo", "body")
          end
        end

        describe "#request" do
          before do
            @connection.stubs(:log)
          end

          it "connects to the server first" do
            @connection.instance_variable_set :@client, (client = mock)
            @connection.expects(:connect!)
            client.stubs(:get).returns(stub(:status => 200, :body => ""))
            @connection.send :request, :get, "/", "query"
          end

          it "queries the server returning the response" do
            @connection.instance_variable_set :@client, (client = mock)
            client.expects(:get).with("/?query=SELECT+1&output_format_write_statistics=1", nil).returns(stub(:status => 200, :body => ""))
            @connection.expects(:parse_body).returns(data = mock)
            assert_equal data, @connection.send(:request, :get, "SELECT 1")
          end

          describe "when not receiving status 200" do
            it "raises a Clickhouse::QueryError" do
              @connection.instance_variable_set :@client, (client = mock)
              client.expects(:get).with("/?query=SELECT+1&output_format_write_statistics=1", nil).returns(stub(:status => 500, :body => ""))
              assert_raises Clickhouse::QueryError do
                @connection.send(:request, :get, "SELECT 1")
              end
            end
          end

          describe "when getting Faraday::Error" do
            it "raises a Clickhouse::ConnectionError" do
              @connection.instance_variable_set :@client, (client = mock)
              client.expects(:get).raises(Faraday::ConnectionFailed.new("Failed to connect"))
              assert_raises Clickhouse::ConnectionError do
                @connection.send(:request, :get, "SELECT 1")
              end
            end
          end

          it "parses the body" do
            json = <<-JSON
              {"meta": []}
            JSON
            @connection.instance_variable_set :@client, (client = mock)
            client.expects(:get).with("/?query=SELECT+1+FORMAT+JSONCompact&output_format_write_statistics=1", nil).returns(stub(:status => 200, :body => json))
            assert_equal({"meta" => []}, @connection.send(:request, :get, "SELECT 1 FORMAT JSONCompact"))
          end
        end

        describe "configuration" do
          describe "database" do
            it "includes the database in the querystring" do
              @connection.instance_variable_get(:@config)[:database] = "system"
              @connection.instance_variable_set(:@client, (client = mock))
              client.expects(:get).with("/?database=system&query=SELECT+1&output_format_write_statistics=1", nil).returns(stub(:status => 200, :body => ""))
              @connection.expects(:parse_body).returns(data = mock)
              assert_equal data, @connection.send(:request, :get, "SELECT 1")
            end
          end

          describe "authentication" do
            it "includes the credentials in the request headers" do
              Faraday::Connection.any_instance.expects(:post).returns(stub(status: 200))
              connection = Clickhouse::Connection.new :password => "awesomepassword"
              connection.connect!
              assert_equal "Basic ZGVmYXVsdDphd2Vzb21lcGFzc3dvcmQ=", connection.send(:client).headers["Authorization"].force_encoding("UTF-8")
            end
          end
        end

        describe "statistics" do
          before do
            @connection = Connection.new
            @json = <<-JSON
              {
                "rows": 1947,
                "statistics": {
                  "elapsed": 0.1882,
                  "rows_read": 1982,
                  "bytes_read": 2003
                }
              }
            JSON
          end

          it "parses the statistics" do
            @connection.stubs(:log)
            @connection.instance_variable_set :@client, (client = mock)
            Time.expects(:now).returns(1882).twice

            client.expects(:get).with("/?query=SELECT+1+FORMAT+JSONCompact&output_format_write_statistics=1", nil).returns(stub(:status => 200, :body => @json))
            @connection.expects(:write_log).with(
              0, "SELECT 1", {
                "elapsed" => "188.2ms",
                "rows_read" => "1.98 thousand",
                "bytes_read" => 2003,
                "rows" => 1947,
                "rows_per_second" => "10.53 thousand",
                "data_per_second" => "10.39 KB",
                "data_read" => "1.96 KB"
              }
            )
            @connection.send(:request, :get, "SELECT 1 FORMAT JSONCompact")
          end

          it "write the expected logs" do
            @connection.instance_variable_set :@client, (client = mock)
            Time.expects(:now).returns(1882).twice

            client.expects(:get).with("/?query=SELECT+1+FORMAT+JSONCompact&output_format_write_statistics=1", nil).returns(stub(:status => 200, :body => @json))
            log = "\n \e[1m\e[35mSQL (0.0ms)\e\e[0m  SELECT 1;\e\n  \e[1m\e[36m1947 rows in set. Elapsed: 188.2ms. Processed: 1.98 thousand rows, 1.96 KB (10.53 thousand rows/s, 10.39 KB/s)\e[0m "

            @connection.expects(:log).with(:debug, log)
            @connection.send(:request, :get, "SELECT 1 FORMAT JSONCompact")
          end

          describe "#number_to_human_duration" do
            it "returns in seconds when more than 1 seconds" do
              assert_equal "2.0s", @connection.send(:number_to_human_duration, 2)
            end
          end
        end
      end

    end
  end
end
