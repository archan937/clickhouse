require_relative "../test_helper"

module Unit
  class TestClickhouse < MiniTest::Test

    describe Clickhouse do
      it "has the current version" do
        version = File.read(path("VERSION")).strip
        assert_equal version, Clickhouse::VERSION
        assert File.read(path "CHANGELOG.md").include?("Version #{version} ")
      end

      describe ".logger=" do
        it "stores the passed value as the instance variable :@logger" do
          Clickhouse.logger = (logger = mock)
          assert_equal logger, Clickhouse.instance_variable_get(:@logger)
        end
      end

      describe ".logger" do
        it "returns its instance variable :@logger" do
          Clickhouse.instance_variable_set :@logger, (logger = mock)
          assert_equal logger, Clickhouse.logger
        end
      end

      describe ".configurations=" do
        it "stores the passed hash as the instance variable :@configurations" do
          Clickhouse.configurations = (configurations = {})
          assert_equal configurations, Clickhouse.instance_variable_get(:@configurations)
        end

        it "stringifies the passed hash" do
          Clickhouse.configurations = {:a => "b"}
          assert_equal({"a" => "b"}, Clickhouse.instance_variable_get(:@configurations))
        end
      end

      describe ".configurations" do
        it "returns its instance variable :@configurations" do
          Clickhouse.instance_variable_set :@configurations, (configurations = mock)
          assert_equal configurations, Clickhouse.configurations
        end
      end

      describe ".establish_connection" do
        describe "when valid" do
          before do
            @connection = mock
            @connection.expects(:connect!)
          end

          it "accepts configuration hashes" do
            config = {"host" => "localhost"}
            Clickhouse::Connection.expects(:new).with(config).returns(@connection)
            Clickhouse.establish_connection config
          end

          it "accepts configuration names" do
            config = {"host" => "localhost"}
            Clickhouse.instance_variable_set(:@configurations, {"foo" => config})
            Clickhouse::Connection.expects(:new).with(config).returns(@connection)
            Clickhouse.establish_connection "foo"
          end

          describe "cluster connections" do
            it "creates a connection pool" do
              config = {:urls => %w(localhost:1234 localhost:1235 localhost:1236)}
              Clickhouse::Cluster.expects(:new).with(config).returns(@connection)
              Clickhouse.establish_connection config
            end
          end
        end

        describe "when invalid" do
          it "denies non-configuration arguments" do
            assert_raises Clickhouse::InvalidConnectionError do
              Clickhouse.establish_connection 123
            end
            assert_raises Clickhouse::InvalidConnectionError do
              Clickhouse.establish_connection true
            end
            assert_raises Clickhouse::InvalidConnectionError do
              Clickhouse.establish_connection "foo"
            end
          end
        end
      end

      describe ".connection" do
        it "returns its instance variable :@connection" do
          Clickhouse.instance_variable_set :@connection, (connection = mock)
          assert_equal connection, Clickhouse.connection
        end
      end
    end

  end
end
