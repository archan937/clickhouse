require_relative "../test_helper"

module Unit
  class TestClickhouse < MiniTest::Test

    describe Clickhouse do
      it "has the current version" do
        version = File.read(path("VERSION")).strip
        assert_equal version, Clickhouse::VERSION
        assert File.read(path "CHANGELOG.md").include?("Version #{version} ")
      end

      describe ".logger" do
        it "returns its instance variable :@logger" do
          Clickhouse.instance_variable_set :@logger, (logger = mock)
          assert_equal logger, Clickhouse.logger
        end
      end

      describe ".logger=" do
        it "stores the passed value as the instance variable :@logger" do
          Clickhouse.logger = (logger = mock)
          assert_equal logger, Clickhouse.instance_variable_get(:@logger)
        end
      end

    end

  end
end
