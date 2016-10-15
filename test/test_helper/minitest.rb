class MiniTest::Test
  def teardown
    Clickhouse.instance_variables.each do |name|
      Clickhouse.instance_variable_set name, nil
    end
  end
end
