#!/usr/bin/env rake

require "bundler/gem_tasks"
require "rake/testtask"

task :default => :test

desc "Run tests and report test coverage to Code Climate"
task :report do
  exec "REPORT=1 rake"
end

Rake::TestTask.new do |test|
  test.pattern = "test/**/test_*.rb"
end
