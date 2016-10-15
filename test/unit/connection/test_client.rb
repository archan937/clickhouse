require_relative "../../test_helper"

module Unit
  module Connection
    class TestClient < MiniTest::Test

      describe Clickhouse::Connection::Client do
        before do
          @connection = Clickhouse::Connection.new
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
end
