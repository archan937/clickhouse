require_relative "../../test_helper"

module Unit
  module Connection
    class TestLogger < MiniTest::Test

      class Connection < SimpleConnection
        include Clickhouse::Connection::Logger
      end

      describe Clickhouse::Connection::Logger do
        before do
          @connection = Connection.new
        end

        describe "#log" do
          describe "when having specified a logger" do
            it "delegates to logger" do
              (logger = mock).expects(:info, "Hello world!")
              Clickhouse.expects(:logger).returns(logger).twice
              @connection.send(:log, :info, "Hello world!")
            end
          end

          describe "when not having specified a logger" do
            it "does nothing" do
              assert_nil @connection.send(:log, :info, "Boo!")
            end
          end
        end
      end

    end
  end
end
