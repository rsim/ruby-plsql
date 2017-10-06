module PLSQL
  class ProcedureCall < SubprogramCall #:nodoc:

    attr_reader :sql
    attr_reader :dbms_output_stream

    def initialize(procedure, args = [], options = {})
      @dbms_output_stream = procedure.schema.dbms_output_stream
      super
    end

    def exec
      # puts "DEBUG: sql = #{@sql.gsub("\n","<br/>\n")}"
      @cursor = @schema.connection.parse(@sql)

      @bind_values.each do |arg, value|
        @cursor.bind_param(":#{arg}", value, @bind_metadata[arg])
      end

      @return_vars.each do |var|
        @cursor.bind_param(":#{var}", nil, @return_vars_metadata[var])
      end

      @cursor.exec

      if block_given?
        yield get_return_value
        nil
      else
        get_return_value
      end
    ensure
      @cursor.close if @cursor
      dbms_output_log
    end

    private

    def construct_sql(args)
      @declare_sql = ""
      @assignment_sql = ""
      @call_sql = ""
      @return_sql = ""
      @return_vars = []
      @return_vars_metadata = {}

      @call_sql << add_return if return_metadata
      # construct procedure call if procedure name is available
      # otherwise will get surrounding call_sql from @procedure (used for table statements)
      if procedure_name
        @call_sql << "#{schema_name}." if schema_name
        @call_sql << "#{package_name}." if package_name
        @call_sql << "#{procedure_name}("
      end

      @bind_values = {}
      @bind_metadata = {}

      # Named arguments
      # there should be just one Hash argument with symbol keys
      if args.size == 1 && args[0].is_a?(Hash) && args[0].keys.all?{|k| k.is_a?(Symbol)} &&
            # do not use named arguments if procedure has just one PL/SQL record PL/SQL table or object type argument -
            # in that case passed Hash should be used as value for this PL/SQL record argument
            # (which will be processed in sequential arguments bracnh)
            !(argument_list.size == 1 &&
              ['PL/SQL RECORD','PL/SQL TABLE','OBJECT'].include?(arguments[(only_argument=argument_list[0])][:data_type]) &&
              args[0].keys != [only_argument])
        # Add missing output arguments with nil value
        arguments.each do |arg, metadata|
          if !args[0].has_key?(arg) && metadata[:in_out] == 'OUT'
            args[0][arg] = nil
          end
        end
        # Add SELF argument if provided
        args[0][:self] = @self if @self
        # Add passed parameters to procedure call with parameter names
        @call_sql << args[0].map do |arg, value|
          "#{arg} => " << add_argument(arg, value)
        end.join(', ')

      # Sequential arguments
      else
        # add SELF as first argument if provided
        args.unshift(@self) if @self
        argument_count = argument_list.size
        raise ArgumentError, "Too many arguments passed to PL/SQL procedure" if args.size > argument_count
        # Add missing output arguments with nil value
        if args.size < argument_count &&
                      (args.size...argument_count).all?{|i| arguments[argument_list[i]][:in_out] == 'OUT'}
          args += [nil] * (argument_count - args.size)
        end
        # Add passed parameters to procedure call in sequence
        @call_sql << (0...args.size).map do |i|
          arg = argument_list[i]
          value = args[i]
          add_argument(arg, value)
        end.join(', ')
      end

      # finish procedure call construction if procedure name is available
      # otherwise will get surrounding call_sql from @procedure (used for table statements)
      if procedure_name
        @call_sql << ");\n"
      else
        @call_sql = @subprogram.call_sql(@call_sql)
      end
      add_out_variables

      @sql = @declare_sql.empty? ? "" : "DECLARE\n" << @declare_sql
      @sql << "BEGIN\n" << @assignment_sql << dbms_output_enable_sql << @call_sql << @return_sql << "END;\n"
    end

    def add_argument(argument, value, argument_metadata=nil)
      argument_metadata ||= arguments[argument]
      raise ArgumentError, "Wrong argument #{argument.inspect} passed to PL/SQL procedure" unless argument_metadata
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        add_record_declaration(argument, argument_metadata)
        record_assignment_sql, record_bind_values, record_bind_metadata =
          record_assignment_sql_values_metadata(argument, argument_metadata, value)
        @assignment_sql << record_assignment_sql
        @bind_values.merge!(record_bind_values)
        @bind_metadata.merge!(record_bind_metadata)
        "l_#{argument}"
      when 'PL/SQL BOOLEAN'
        @declare_sql << "l_#{argument} BOOLEAN;\n"
        @assignment_sql << "l_#{argument} := (:#{argument} = 1);\n"
        @bind_values[argument] = value.nil? ? nil : (value ? 1 : 0)
        @bind_metadata[argument] = argument_metadata.merge(:data_type => "NUMBER", :data_precision => 1)
        "l_#{argument}"
      when 'UNDEFINED'
        if argument_metadata[:type_name] == 'XMLTYPE'
          @declare_sql << "l_#{argument} XMLTYPE;\n"
          @assignment_sql << "l_#{argument} := XMLTYPE(:#{argument});\n" if not value.nil?
          @bind_values[argument] = value if not value.nil?
          @bind_metadata[argument] = argument_metadata.merge(:data_type => "CLOB")
          "l_#{argument}"
        end
      else
        # TABLE or PL/SQL TABLE type defined inside package
        if argument_metadata[:tmp_table_name]
          add_table_declaration_and_assignment(argument, argument_metadata)
          insert_values_into_tmp_table(argument, argument_metadata, value)
          "l_#{argument}"
        else
          @bind_values[argument] = value
          @bind_metadata[argument] = argument_metadata
          ":#{argument}"
        end
      end
    end

    def add_table_declaration_and_assignment(argument, argument_metadata)
      is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
      @declare_sql << "l_#{argument} #{argument_metadata[:sql_type_name]}#{is_index_by_table ? nil : " := #{argument_metadata[:sql_type_name]}()"};\n"
      @assignment_sql << "FOR r_#{argument} IN c_#{argument} LOOP\n"
      @assignment_sql << "l_#{argument}.EXTEND;\n" unless is_index_by_table
      case argument_metadata[:element][:data_type]
      when 'PL/SQL RECORD'
        fields = record_fields_sorted_by_position(argument_metadata[:element][:fields])
        fields_string = is_index_by_table ? "*" : fields.join(', ')
        @declare_sql << "CURSOR c_#{argument} IS SELECT #{fields_string} FROM #{argument_metadata[:tmp_table_name]} ORDER BY i__;\n"
        if is_index_by_table
          fields.each do |field|
            @assignment_sql << "l_#{argument}(r_#{argument}.i__).#{field} := r_#{argument}.#{field};\n"
          end
        else
          @assignment_sql << "l_#{argument}(l_#{argument}.COUNT) := r_#{argument};\n"
        end
      else
        @declare_sql << "CURSOR c_#{argument} IS SELECT * FROM #{argument_metadata[:tmp_table_name]} ORDER BY i__;\n"
        @assignment_sql << "l_#{argument}(r_#{argument}.i__) := r_#{argument}.element;\n"
      end
      @assignment_sql << "END LOOP;\n"
      @assignment_sql << "DELETE FROM #{argument_metadata[:tmp_table_name]};\n"
    end

    def insert_values_into_tmp_table(argument, argument_metadata, values)
      return unless values && !values.empty?
      is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
      if is_index_by_table
        raise ArgumentError, "Hash value should be passed for #{argument.inspect} argument" unless values.is_a?(Hash)
      else
        raise ArgumentError, "Array value should be passed for #{argument.inspect} argument" unless values.is_a?(Array)
      end
      tmp_table = @schema.root_schema.send(argument_metadata[:tmp_table_name])
      # insert values without autocommit
      old_autocommit = @schema.connection.autocommit?
      @schema.connection.autocommit = false if old_autocommit
      tmp_table.delete
      case argument_metadata[:element][:data_type]
      when 'PL/SQL RECORD'
        values_with_index = []
        if is_index_by_table
          values.each{|i,v| values_with_index << v.merge(:i__ => i)}
        else
          values.each_with_index{|v,i| values_with_index << v.merge(:i__ => i+1)}
        end
        tmp_table.insert values_with_index
      else
        values_with_index = []
        if is_index_by_table
          values.each{|i,v| values_with_index << [v, i]}
        else
          values.each_with_index{|v,i| values_with_index << [v, i+1]}
        end
        tmp_table.insert_values [:element, :i__], *values_with_index
      end
      @schema.connection.autocommit = true if old_autocommit
    end

    def add_record_declaration(argument, argument_metadata)
      @declare_sql << if argument_metadata[:type_subname]
        "l_#{argument} #{argument_metadata[:sql_type_name]};\n"
      else
        fields_metadata = argument_metadata[:fields]
        sql = "TYPE t_#{argument} IS RECORD (\n"
        sql << record_fields_sorted_by_position(fields_metadata).map do |field|
          metadata = fields_metadata[field]
          "#{field} #{type_to_sql(metadata)}"
        end.join(",\n")
        sql << ");\n"
        sql << "l_#{argument} t_#{argument};\n"
      end
    end

    def record_fields_sorted_by_position(fields_metadata)
      fields_metadata.keys.sort_by{|k| fields_metadata[k][:position]}
    end

    def record_assignment_sql_values_metadata(argument, argument_metadata, record_value)
      sql = ""
      bind_values = {}
      bind_metadata = {}
      (record_value||{}).each do |key, value|
        field = key.is_a?(Symbol) ? key : key.to_s.downcase.to_sym
        metadata = argument_metadata[:fields][field]
        raise ArgumentError, "Wrong field name #{key.inspect} passed to PL/SQL record argument #{argument.inspect}" unless metadata
        bind_variable = :"#{argument}_f#{metadata[:position]}"
        case metadata[:data_type]
        when 'PL/SQL BOOLEAN'
          sql << "l_#{argument}.#{field} := (:#{bind_variable} = 1);\n"
          bind_values[bind_variable] = value.nil? ? nil : (value ? 1 : 0)
          bind_metadata[bind_variable] = metadata.merge(:data_type => "NUMBER", :data_precision => 1)
        else
          sql << "l_#{argument}.#{field} := :#{bind_variable};\n"
          bind_values[bind_variable] = value
          bind_metadata[bind_variable] = metadata
        end
      end
      [sql, bind_values, bind_metadata]
    end

    def add_return
      add_return_variable(:return, return_metadata, true)
    end

    def add_out_variables
      out_list.each do |argument|
        add_return_variable(argument, arguments[argument])
      end
    end

    def add_return_variable(argument, argument_metadata, is_return_value=false)
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        add_record_declaration(argument, argument_metadata) if is_return_value
        argument_metadata[:fields].each do |field, metadata|
          # should use different output bind variable as JDBC does not support
          # if output bind variable appears in several places
          bind_variable = :"#{argument}_o#{metadata[:position]}"
          case metadata[:data_type]
          when 'PL/SQL BOOLEAN'
            @return_vars << bind_variable
            @return_vars_metadata[bind_variable] = metadata.merge(:data_type => "NUMBER", :data_precision => 1)
            arg_field = "l_#{argument}.#{field}"
            @return_sql << ":#{bind_variable} := " << "CASE WHEN #{arg_field} = true THEN 1 " <<
                                                      "WHEN #{arg_field} = false THEN 0 ELSE NULL END;\n"
          else
            @return_vars << bind_variable
            @return_vars_metadata[bind_variable] = metadata
            @return_sql << ":#{bind_variable} := l_#{argument}.#{field};\n"
          end
        end
        "l_#{argument} := " if is_return_value
      when 'UNDEFINED'
        if argument_metadata[:type_name] == 'XMLTYPE'
          @declare_sql << "l_#{argument} XMLTYPE;\n" if is_return_value
          bind_variable = :"o_#{argument}"
          @return_vars << bind_variable
          @return_vars_metadata[bind_variable] = argument_metadata.merge(:data_type => "CLOB")
          @return_sql << ":#{bind_variable} := CASE WHEN l_#{argument} IS NOT NULL THEN l_#{argument}.getclobval() END;\n"
          "l_#{argument} := " if is_return_value
        end
      when 'PL/SQL BOOLEAN'
        @declare_sql << "l_#{argument} BOOLEAN;\n" if is_return_value
        @declare_sql << "o_#{argument} NUMBER(1);\n"
        # should use different output bind variable as JDBC does not support
        # if output bind variable appears in several places
        bind_variable = :"o_#{argument}"
        @return_vars << bind_variable
        @return_vars_metadata[bind_variable] = argument_metadata.merge(:data_type => "NUMBER", :data_precision => 1)
        @return_sql << "IF l_#{argument} IS NULL THEN\no_#{argument} := NULL;\n" <<
                      "ELSIF l_#{argument} THEN\no_#{argument} := 1;\nELSE\no_#{argument} := 0;\nEND IF;\n" <<
                      ":#{bind_variable} := o_#{argument};\n"
        "l_#{argument} := " if is_return_value
      else
        if argument_metadata[:tmp_table_name]
          add_return_table(argument, argument_metadata, is_return_value)
        elsif is_return_value
          @return_vars << argument
          @return_vars_metadata[argument] = argument_metadata
          ":#{argument} := "
        end
      end
    end

    def add_return_table(argument, argument_metadata, is_return_value=false)
      is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
      declare_i__
      @declare_sql << "l_return #{return_metadata[:sql_type_name]};\n" if is_return_value
      @return_vars << argument
      @return_vars_metadata[argument] = argument_metadata.merge(:data_type => "REF CURSOR")
      @return_sql << if is_index_by_table
        "i__ := l_#{argument}.FIRST;\nLOOP\nEXIT WHEN i__ IS NULL;\n"
      else
        "IF l_#{argument}.COUNT > 0 THEN\nFOR i__ IN l_#{argument}.FIRST..l_#{argument}.LAST LOOP\n"
      end
      case argument_metadata[:element][:data_type]
      when 'PL/SQL RECORD'
        field_names = record_fields_sorted_by_position(argument_metadata[:element][:fields])
        values_string = field_names.map{|f| "l_#{argument}(i__).#{f}"}.join(', ')
        @return_sql << "INSERT INTO #{argument_metadata[:tmp_table_name]} VALUES (#{values_string}, i__);\n"
        return_fields_string = is_index_by_table ? '*' : field_names.join(', ')
      else
        @return_sql << "INSERT INTO #{argument_metadata[:tmp_table_name]} VALUES (l_#{argument}(i__), i__);\n"
        return_fields_string = '*'
      end
      @return_sql << "i__ := l_#{argument}.NEXT(i__);\n" if is_index_by_table
      @return_sql << "END LOOP;\n"
      @return_sql << "END IF;\n" unless is_index_by_table
      @return_sql << "OPEN :#{argument} FOR SELECT #{return_fields_string} FROM #{argument_metadata[:tmp_table_name]} ORDER BY i__;\n"
      @return_sql << "DELETE FROM #{argument_metadata[:tmp_table_name]};\n"
      "l_#{argument} := " if is_return_value
    end

    # declare once temp variable i__ that is used as itertor
    def declare_i__
      unless @declared_i__
        @declare_sql << "i__ PLS_INTEGER;\n"
        @declared_i__ = true
      end
    end

    def type_to_sql(metadata)
      ProcedureCommon.type_to_sql(metadata)
    end

    def get_return_value
      # if function with output parameters
      if return_metadata && out_list.size > 0
        result = [function_return_value, {}]
        out_list.each do |k|
          result[1][k] = out_variable_value(k)
        end
        result
      # if function without output parameters
      elsif return_metadata
        function_return_value
      # if procedure with output parameters
      elsif out_list.size > 0
        result = {}
        out_list.each do |k|
          result[k] = out_variable_value(k)
        end
        result
      # if procedure without output parameters
      else
        nil
      end
    end

    def function_return_value
      return_variable_value(:return, return_metadata)
    end

    def out_variable_value(argument)
      return_variable_value(argument, arguments[argument])
    end

    def return_variable_value(argument, argument_metadata)
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        return_value = {}
        argument_metadata[:fields].each do |field, metadata|
          field_value = @cursor[":#{argument}_o#{metadata[:position]}"]
          case metadata[:data_type]
          when 'PL/SQL BOOLEAN'
            return_value[field] = field_value.nil? ? nil : field_value == 1
          else
            return_value[field] = field_value
          end
        end
        return_value
      when 'PL/SQL BOOLEAN'
        numeric_value = @cursor[":o_#{argument}"]
        numeric_value.nil? ? nil : numeric_value == 1
      when 'UNDEFINED'
        if argument_metadata[:type_name] == 'XMLTYPE'
          @cursor[":o_#{argument}"]
        end
      else
        if argument_metadata[:tmp_table_name]
          is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
          case argument_metadata[:element][:data_type]
          when 'PL/SQL RECORD'
            if is_index_by_table
              Hash[*@cursor[":#{argument}"].fetch_hash_all.map{|row| [row.delete(:i__), row]}.flatten]
            else
              @cursor[":#{argument}"].fetch_hash_all
            end
          else
            if is_index_by_table
              Hash[*@cursor[":#{argument}"].fetch_all.map{|row| [row[1], row[0]]}.flatten]
            else
              @cursor[":#{argument}"].fetch_all.map{|row| row[0]}
            end
          end
        else
          @cursor[":#{argument}"]
        end
      end
    end

    def arguments
      @arguments ||= overload_arguments[@overload]
    end

    def procedure_name
      subprogram_name
    end

    def dbms_output_enable_sql
      @dbms_output_stream ? "DBMS_OUTPUT.ENABLE(#{@schema.dbms_output_buffer_size});\n" : ""
    end

    def dbms_output_lines
      lines = []
      if @dbms_output_stream
        if (@schema.connection.database_version <=> [10, 2, 0, 0]) >= 0
          cursor = @schema.connection.parse("BEGIN DBMS_OUTPUT.GET_LINES(:dbms_output_lines, :dbms_output_numlines); END;\n")
          cursor.bind_param(':dbms_output_lines', nil,
                            :data_type => 'TABLE',
                            :data_length => nil,
                            :sql_type_name => "SYS.DBMSOUTPUT_LINESARRAY",
                            :in_out => 'OUT')
          cursor.bind_param(':dbms_output_numlines', Schema::DBMS_OUTPUT_MAX_LINES, :data_type => 'NUMBER', :in_out => 'IN/OUT')
          cursor.exec
          lines = cursor[':dbms_output_lines']
          cursor.close
        else
          cursor = @schema.connection.parse("BEGIN sys.dbms_output.get_line(:line, :status); END;")
          while true do
            cursor.bind_param(':line', nil, :data_type => 'VARCHAR2', :in_out => 'OUT')
            cursor.bind_param(':status', nil, :data_type => 'NUMBER', :in_out => 'OUT')
            cursor.exec
            break unless cursor[':status'] == 0
            lines << cursor[':line']
          end
          cursor.close
        end
      end
      lines
    end

    def dbms_output_log
      dbms_output_lines.each do |line|
        @dbms_output_stream.puts "DBMS_OUTPUT: #{line}" if line
      end
      @dbms_output_stream.flush if @dbms_output_stream
    end

  end

end
