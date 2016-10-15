require_relative "../test_helper"

module Unit
  class TestConnection < MiniTest::Test

    describe Clickhouse::Connection do
      before do
        @connection = Clickhouse::Connection.new
      end

      describe "#initialize" do
        describe "when not passing a configuration" do
          it "uses a default configuration" do
            assert_equal({
              :scheme => "http",
              :host => "localhost",
              :port => 8123
            }, @connection.instance_variable_get(:@config))
          end
        end

        describe "when passing a configuration" do
          it "overrides the default configuration" do
            connection = Clickhouse::Connection.new :scheme => "https", "host" => "19.82.8.1"
            assert_equal({
              :scheme => "https",
              :host => "19.82.8.1",
              :port => 8123
            }, connection.instance_variable_get(:@config))
          end
        end
      end

      describe "#ping!" do
        describe "when failed to connect" do
          it "returns true" do
            Faraday::Connection.any_instance.expects(:get).raises(Faraday::ConnectionFailed.new("Failed to connect"))
            assert_raises Clickhouse::ConnectionError do
              @connection.ping!
            end
          end
        end

        describe "when receiving 200" do
          it "returns true" do
            Faraday::Connection.any_instance.expects(:get).returns(stub({:status => 200}))
            assert_equal true, @connection.ping!
          end
        end

        describe "when receiving 500" do
          it "raises a Clickhouse::ConnectionError" do
            Faraday::Connection.any_instance.expects(:get).returns(stub({:status => 500}))
            assert_raises Clickhouse::ConnectionError do
              @connection.ping!
            end
          end
        end
      end
    end

  end
end
