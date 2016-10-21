require_relative "../../test_helper"

module Unit
  module Connection
    class TestCluser < MiniTest::Test

      describe Clickhouse::Cluster do
        it "creates a connection pond" do
          cluster = Clickhouse::Cluster.new :urls => %w(localhost:1234 localhost:1235 localhost:1236)
          assert_equal true, cluster.pond.is_a?(Pond)
        end

        describe "when connection succeeds" do
          it "keeps valid connections from the pond" do
            Clickhouse::Connection.any_instance.expects(:tables)
            Clickhouse::Connection.any_instance.expects(:ping!)

            cluster = Clickhouse::Cluster.new :urls => %w(http://localhost:1234 http://localhost:1235 http://localhost:1236)
            assert_equal %w(
              http://localhost:1234
              http://localhost:1235
              http://localhost:1236
            ), cluster.pond.available.collect(&:url)

            cluster.tables
            assert_equal %w(
              http://localhost:1235
              http://localhost:1236
              http://localhost:1234
            ), cluster.pond.available.collect(&:url)
          end
        end

        describe "when connection fails" do
          it "removes invalid connections from the pond" do
            cluster = Clickhouse::Cluster.new :urls => %w(http://localhost:1234 http://localhost:1235 http://localhost:1236)

            assert_equal %w(
              http://localhost:1234
              http://localhost:1235
              http://localhost:1236
            ), cluster.pond.available.collect(&:url)

            cluster.tables
            assert_equal [], cluster.pond.available.collect(&:url)
          end
        end

        describe "when error gets raised other than Clickhouse::ConnectionError" do
          it "does not remove the connection from the pond" do
            Clickhouse::Connection.any_instance.expects(:ping!)

            cluster = Clickhouse::Cluster.new :urls => %w(http://localhost:1234 http://localhost:1235 http://localhost:1236)
            assert_equal %w(
              http://localhost:1234
              http://localhost:1235
              http://localhost:1236
            ), cluster.pond.available.collect(&:url)

            assert_raises NoMethodError do
              cluster.select_rows ""
            end

            assert_equal %w(
              http://localhost:1235
              http://localhost:1236
              http://localhost:1234
            ), cluster.pond.available.collect(&:url)
          end
        end
      end

    end
  end
end
