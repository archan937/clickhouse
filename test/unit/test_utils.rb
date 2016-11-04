require_relative "../test_helper"

module Unit
  class TestUtils < MiniTest::Test

    describe Clickhouse::Utils do
      describe ".normalize_url" do
        describe "when passing scheme" do
          it "returns the passed url" do
            assert_equal "paul://engel", Clickhouse::Utils.normalize_url("paul://engel")
          end
        end

        describe "when not passing scheme" do
          it "prepends the default scheme" do
            assert_equal "http://engel", Clickhouse::Utils.normalize_url("engel")
          end
        end
      end

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
