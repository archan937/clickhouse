require "thor"
require "clickhouse"

module Clickhouse
  class CLI < Thor

    DEPENDENCIES = {:server => "sinatra"}
    DEFAULT_URLS = "http://localhost:8123"

    desc "server", "Start a Sinatra server as a ClickHouse client"
    method_options [:port, "-p"] => 1982, [:username, "-u"] => :string, [:password, "-P"] => :string
    def server(urls = DEFAULT_URLS)
      run! :server, urls, options
    end

    desc "console", "Start a Pry console as a ClickHouse client"
    method_options [:username, "-u"] => :string, [:password, "-P"] => :string
    def console(urls = DEFAULT_URLS)
      run! :console, urls, options
    end

    map "s" => :server
    map "c" => :console

  private

    def run!(const, urls, options)
      require! DEPENDENCIES[const]
      require_relative "cli/#{const}"
      connect! options.merge(:urls => urls)
      self.class.const_get(const.to_s.capitalize).run!(options)
    end

    def require!(name)
      require(name) if name
    rescue LoadError
      puts "fatal: #{name.capitalize} not available. Please run `gem install #{name}` first."
      exit!
    end

    def connect!(urls)
      Clickhouse.establish_connection :urls => urls.split(",")
    end

    def method_missing(method, *args)
      raise Error, "Unrecognized command \"#{method}\". Please consult `clickhouse help`."
    end

  end
end
