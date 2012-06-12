module PLSQL
  class ProcedureCall < SubprogramCall #:nodoc:
    attr_reader :output_stream

    def initialize(procedure, args = [], options = {})
      @output_stream = procedure.schema.dbms_output_stream
      super
    end

    def exec
      # puts "DEBUG: sql = #{@sql.gsub("\n","<br/>\n")}"
      @cursor = @schema.connection.parse(@sql)

      @binds[:values].each do |arg, value|
        @cursor.bind_param(":#{arg}", value, @binds[:metadata][arg])
      end

      @return[:variables].each do |var|
        @cursor.bind_param(":#{var}", nil, @return[:metadata][var])
      end

      @cursor.exec

      dbms_output_log

      if block_given?
        yield get_return_value
        nil
      else
        get_return_value
      end
    ensure
      @cursor.close if @cursor
    end

    private

    def construct_sql(arguments)
      prepare_sql_construction
      call_sql = ""
      call_sql << add_return if return_metadata

      # construct procedure call if procedure name is available
      # otherwise will get surrounding call_sql from @procedure (used for table statements)
      if subprogram_name
        call_sql << "#{full_subprogram_name}(#{add_arguments(arguments)});\n"
      else
        call_sql << add_arguments(arguments)
        call_sql = @subprogram.call_sql(call_sql)
      end

      add_out_variables

      dbms_output_enable_sql, dbms_output_get_sql = dbms_output_sql

      @sql = <<-SQL
        DECLARE
          #{@declare_sql}
        BEGIN
          #{@assignment_sql}
          #{dbms_output_enable_sql}
          #{call_sql}
          #{dbms_output_get_sql}
          #{@return[:sql]}
        END;
      SQL
    end

    def get_return_value
      # create output hash if there are any out variables
      output = out_list.inject({}) {|res, k| res[k] = out_variable_value(k); res} if out_list.size > 0
      # if function with output parameters
      if return_metadata && out_list.size > 0
        [function_return_value, output]
      # if function without output parameters
      elsif return_metadata
        function_return_value
      # if procedure with output parameters
      elsif out_list.size > 0
        output
      end
      # nil if procedure without output parameters
    end

    def dbms_output_sql
      return ["", ""] unless output_stream

      dbms_output_enable_sql = "DBMS_OUTPUT.ENABLE(#{@schema.dbms_output_buffer_size});\n"
      # if database version is at least 10.2 then use DBMS_OUTPUT.GET_LINES with SYS.DBMSOUTPUT_LINESARRAY
      if (@schema.connection.database_version <=> [10, 2, 0, 0]) >= 0
        add_variable_declaration('dbms_output_numlines', 'integer', :value => Schema::DBMS_OUTPUT_MAX_LINES)
        dbms_output_get_sql = "DBMS_OUTPUT.GET_LINES(:dbms_output_lines, l_dbms_output_numlines);\n"
        bind_value(:dbms_output_lines, nil,
                   :data_type => 'TABLE', :data_length => nil,
                   :sql_type_name => "SYS.DBMSOUTPUT_LINESARRAY", :in_out => 'OUT')
      # if database version is less than 10.2 then use individual DBMS_OUTPUT.GET_LINE calls
      else
        dbms_output_get_sql = ""
      end
      [dbms_output_enable_sql, dbms_output_get_sql]
    end

    def dbms_output_log
      return unless output_stream

      # if database version is at least 10.2 then :dbms_output_lines output bind variable has dbms_output lines
      if @binds[:metadata][:dbms_output_lines]
        @cursor[':dbms_output_lines'].each {|line| output_stream.puts("DBMS_OUTPUT: #{line}") if line}
      # if database version is less than 10.2 then use individual DBMS_OUTPUT.GET_LINE calls
      else
        cursor = @schema.connection.parse("BEGIN sys.dbms_output.get_line(:line, :status); END;")
        while true do
          cursor.bind_param(':line', nil, :data_type => 'VARCHAR2', :in_out => 'OUT')
          cursor.bind_param(':status', nil, :data_type => 'NUMBER', :in_out => 'OUT')
          cursor.exec
          break unless cursor[':status'] == 0
          output_stream.puts "DBMS_OUTPUT: #{cursor[':line']}"
        end
        cursor.close
      end
      output_stream.flush
    end
  end
end