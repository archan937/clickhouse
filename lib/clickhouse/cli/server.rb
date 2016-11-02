require "tilt/erubis"

module Clickhouse
  class CLI < Thor
    class Server < Sinatra::Base

      set :views, File.expand_path("../server/views", __FILE__)
      set :public_folder, File.expand_path("../server/assets", __FILE__)

      get "/" do
        erb :index
      end

    end
  end
end
