require_relative "test_helper/coverage"

require "minitest"
require "minitest/autorun"
require "mocha/setup"

def path(path)
  File.expand_path "../../#{path}", __FILE__
end

require "bundler"
Bundler.require :default, :development, :test
