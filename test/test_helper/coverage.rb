if Dir.pwd == File.expand_path("../../..", __FILE__)
  require "simplecov"
  SimpleCov.coverage_dir "test/coverage"
  SimpleCov.start do
    add_group "Clickhouse", "lib"
    add_group "Test suite", "test"
  end
end
