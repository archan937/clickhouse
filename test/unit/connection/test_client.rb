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
        end

        describe "#connect!" do
          describe "when failed to connect" do
            it "returns true" do
              Faraday::Connection.any_instance.expects(:get).raises(Faraday::ConnectionFailed.new("Failed to connect"))
              assert_raises Clickhouse::ConnectionError do
                @connection.connect!
              end
            end
          end

          describe "when receiving 200" do
            it "returns true" do
              Faraday::Connection.any_instance.expects(:get).returns(stub({:status => 200}))
              assert_equal true, @connection.connect!
            end
          end

          describe "when receiving 500" do
            it "raises a Clickhouse::ConnectionError" do
              Faraday::Connection.any_instance.expects(:get).returns(stub({:status => 500}))
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
          it "gets delegated to the client" do
            @connection.instance_variable_set :@client, (client = mock)
            client.expects(:get).with(:foo)
            @connection.get(:foo)
          end
        end

        describe "#post" do
          it "gets delegated to the client" do
            @connection.instance_variable_set :@client, (client = mock)
            client.expects(:post).with(:foo)
            @connection.post(:foo)
          end
        end
      end

    end
  end
end
