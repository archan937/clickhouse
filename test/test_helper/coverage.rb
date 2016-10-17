if Dir.pwd == File.expand_path("../../..", __FILE__)
  if ENV["REPORT"].to_i == 1
    require "dotenv"
    Dotenv.load

    require "codeclimate-test-reporter"
    CodeClimate::TestReporter.start
  end

  require "simplecov"
  SimpleCov.coverage_dir "test/coverage"
  SimpleCov.start do
    add_group "Clickhouse", "lib"
    add_group "Test suite", "test"
  end
end
