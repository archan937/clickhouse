module Clickhouse
  class CLI < Thor
    class Server < Sinatra::Base

      get "/" do
        "Welcome to the Clickhouse client"
      end

    end
  end
end
