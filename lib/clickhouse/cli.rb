require "thor"
require "launchy"
require "clickhouse"

module Clickhouse
  class CLI < Thor

    DEPENDENCIES = {:server => "sinatra"}
    DEFAULT_URLS = "http://localhost:8123"

    desc "server", "Start a Sinatra server as a ClickHouse client"
    method_options [:port, "-p"] => 1982, [:username, "-u"] => :string, [:password, "-P"] => :string
    def server(urls = DEFAULT_URLS)
      run! :server, urls, options do
        Launchy.open "http://localhost:#{options[:port]}"
      end
    end

    desc "console", "Start a Pry console as a ClickHouse client"
    method_options [:username, "-u"] => :string, [:password, "-P"] => :string
    def console(urls = DEFAULT_URLS)
      run! :console, urls, options
    end

    map "s" => :server
    map "c" => :console

  private

    def run!(const, urls, options, &block)
      require! DEPENDENCIES[const]
      require_relative "cli/client"
      require_relative "cli/#{const}"
      connect! urls, options
      self.class.const_get(const.to_s.capitalize).run!(:port => options["port"], &block)
    end

    def require!(name)
      require(name) if name
    rescue LoadError
      puts "fatal: #{name.capitalize} not available. Please run `gem install #{name}` first."
      exit!
    end

    def connect!(urls, options)
      config = options.merge(:urls => urls.split(",")).inject({}){|h, (k, v)| h[k.to_sym] = v; h}
      Clickhouse.establish_connection config
    end

    def method_missing(method, *args)
      raise Error, "Unrecognized command \"#{method}\". Please consult `clickhouse help`."
    end

  end
end
