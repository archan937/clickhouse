# -*- encoding: utf-8 -*-
require File.expand_path("../lib/clickhouse/version", __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Paul Engel"]
  gem.email         = ["pm_engel@icloud.com"]
  gem.summary       = %q{A Ruby database driver for Clickhouse}
  gem.description   = %q{A Ruby database driver for Clickhouse}
  gem.homepage      = "https://github.com/archan937/clickhouse"

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.name          = "clickhouse"
  gem.require_paths = ["lib"]
  gem.version       = Clickhouse::VERSION

  gem.add_dependency "faraday"

  gem.add_development_dependency "rake"
  gem.add_development_dependency "pry"
  gem.add_development_dependency "dotenv"
  gem.add_development_dependency "codeclimate-test-reporter"
  gem.add_development_dependency "simplecov"
  gem.add_development_dependency "minitest"
  gem.add_development_dependency "mocha"
end
