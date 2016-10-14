require_relative "../test_helper"

module Unit
  class TestClickhouse < MiniTest::Test

    describe Clickhouse do
      it "has the current version" do
        version = File.read(path("VERSION")).strip
        assert_equal version, Clickhouse::VERSION
        assert File.read(path "CHANGELOG.md").include?("Version #{version} ")
      end
    end

  end
end
