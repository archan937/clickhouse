require_relative "../test_helper"

module Unit
  class TestConnection < MiniTest::Test

    describe Clickhouse::Connection do
      before do
        @connection = Clickhouse::Connection.new
      end

      describe "included modules" do
        it "includes all Clickhouse::Connection modules" do
          assert_equal true, @connection.class.included_modules.include?(Clickhouse::Connection::Client)
        end
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
    end

  end
end
