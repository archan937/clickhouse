class SimpleConnection
  attr_reader :config

  def initialize
    @config = {
      :scheme => "http",
      :host => "localhost",
      :port => 8123
    }
  end

end
