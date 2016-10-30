require "readline"

module Clickhouse
  class CLI < Thor
    class Console

      HISTORY_FILE = "#{ENV["HOME"]}/.clickhouse_history"

      def self.run!(options = {})
        Clickhouse.logger = self
        load_history
        readline
      end

      def self.info(message)
        @log = message.split("\n").detect{|line| line.include?("36m")}
      end

    # private

      def self.load_history
        File.readlines(HISTORY_FILE).each do |line|
          Readline::HISTORY.push line.gsub(";;", "\n")
        end if File.exists?(HISTORY_FILE)
      end

      def self.alter_history(buffer)
        Readline::HISTORY.pop
        if buffer
          Readline::HISTORY.pop
          Readline::HISTORY.push(buffer)
        end
      end

      def self.dump_history
        File.open(HISTORY_FILE, "w+") do |file|
          Readline::HISTORY.each do |line|
            file << line.gsub("\n", ";;")
          end
        end
      end

      def self.readline(buffer = nil)
        prompt = buffer ? ":-] " : ":) "
        line = Readline.readline(prompt, true)
        exit! unless line
        sql = [buffer, line].compact.join("\n")
        alter_history(sql)
        readline execute(sql)
      end

      def self.execute(sql)
        if sql[-1] == ";"
          dump_history
          print_result Clickhouse.connection.query(sql[0..-2])
          @log = nil
        else
          sql
        end
      end

      def self.print_result(result_set)
        array = [result_set.names].concat(result_set.to_a)

        lengths = array.inject([]) do |lengths, row|
          row.each_with_index do |value, index|
            length = value.to_s.length
            lengths[index] = [lengths[index].to_i, length].max
          end
          lengths
        end

        puts

        array.each do |row|
          values = [nil]
          lengths.each_with_index do |length, index|
            values << row[index].to_s.ljust(length, " ")
          end
          values << nil
          puts values.join(" | ")
        end

        if @log
          puts
          puts @log.strip
        end

        puts
      end

      private_class_method :load_history
      private_class_method :alter_history
      private_class_method :dump_history
      private_class_method :readline
      private_class_method :execute
      private_class_method :print_result

    end
  end
end
