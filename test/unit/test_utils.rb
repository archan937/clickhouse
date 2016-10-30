require_relative "../test_helper"

module Unit
  class TestUtils < MiniTest::Test

    describe Clickhouse::Utils do
      describe ".extract_format" do
        describe "when not having a format" do
          it "returns the query and nil as format" do
            assert_equal ["SELECT 1", nil], Clickhouse::Utils.extract_format("SELECT 1")
          end
        end

        it "strips the query" do
          assert_equal ["SELECT 1", nil], Clickhouse::Utils.extract_format("SELECT 1     ")
        end

        it "extracts the format from the query" do
          assert_equal ["SELECT 1", "SomeFormat"], Clickhouse::Utils.extract_format("SELECT 1 FORMAT SomeFormat")
        end
      end
    end

  end
end
