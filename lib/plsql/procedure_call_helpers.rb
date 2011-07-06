module PLSQL
  
  module ProcedureCallHelperProvider
    
    def procedure_call_helper(dialect)
      case dialect
      when :oracle
        ORAProcedureCallHelper
      when :postgres
        PGProcedureCallHelper
      end
    end
    
  end
  
  module ORAProcedureCallHelper
    
    MATCHING_TYPES = {
      :integer => ['NUMBER', 'PLS_INTEGER', 'BINARY_INTEGER'],
      :decimal => ['NUMBER', 'BINARY_FLOAT', 'BINARY_DOUBLE'],
      :string => ['VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR', 'CLOB', 'BLOB'],
      :date => ['DATE'],
      :time => ['DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE'],
      :boolean => ['PL/SQL BOOLEAN'],
      :hash => ['PL/SQL RECORD', 'OBJECT', 'PL/SQL TABLE'],
      :array => ['TABLE', 'VARRAY'],
      :cursor => ['REF CURSOR']
    }
    
    def matching_db_types_for_ruby_value(value)
      case value
      when NilClass
        :all
      when Fixnum, Bignum
        MATCHING_TYPES[:integer]
      when BigDecimal, Float
        MATCHING_TYPES[:decimal]
      when String
        MATCHING_TYPES[:string]
      when Date
        MATCHING_TYPES[:date]
      when Time
        MATCHING_TYPES[:time]
      when TrueClass, FalseClass
        MATCHING_TYPES[:boolean]
      when Hash
        MATCHING_TYPES[:hash]
      when Array
        MATCHING_TYPES[:array]
      when CursorCommon
        MATCHING_TYPES[:cursor]
      end
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
        @call_sql << args.each_with_index.map do |value, i|
          arg = argument_list[i]
          add_argument(arg, value)
        end.join(', ')
      end

      # finish procedure call construction if procedure name is available
      # otherwise will get surrounding call_sql from @procedure (used for table statements)
      if procedure_name
        @call_sql << ");\n"
      else
        @call_sql = @procedure.call_sql(@call_sql)
      end
      add_out_variables

      dbms_output_enable_sql, dbms_output_get_sql = dbms_output_sql

      @sql = @declare_sql.empty? ? "" : "DECLARE\n" << @declare_sql
      @sql << "BEGIN\n" << @assignment_sql << dbms_output_enable_sql << @call_sql << dbms_output_get_sql << @return_sql << "END;\n"
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
    
    def record_assignment_sql_values_metadata(argument, argument_metadata, record_value)
      sql = ""
      bind_values = {}
      bind_metadata = {}
      (record_value||{}).each do |key, value|
        field = key.is_a?(Symbol) ? key : key.to_s.downcase.to_sym
        metadata = argument_metadata[:fields][field]
        raise ArgumentError, "Wrong field name #{key.inspect} passed to PL/SQL record argument #{argument.inspect}" unless metadata
        bind_variable = :"#{argument}_f#{metadata[:position]}"
        sql << "l_#{argument}.#{field} := :#{bind_variable};\n"
        bind_values[bind_variable] = value
        bind_metadata[bind_variable] = metadata
      end
      [sql, bind_values, bind_metadata]
    end

    def add_return_variable(argument, argument_metadata, is_return_value=false)
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        add_record_declaration(argument, argument_metadata) if is_return_value
        argument_metadata[:fields].each do |field, metadata|
          # should use different output bind variable as JDBC does not support
          # if output bind variable appears in several places
          bind_variable = :"#{argument}_o#{metadata[:position]}"
          @return_vars << bind_variable
          @return_vars_metadata[bind_variable] = metadata
          @return_sql << ":#{bind_variable} := l_#{argument}.#{field};\n"
        end
        "l_#{argument} := " if is_return_value
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
    
    def type_to_sql(metadata)
      ProcedureCommon.type_to_sql(metadata)
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
        "FOR i__ IN l_#{argument}.FIRST..l_#{argument}.LAST LOOP\n"
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
    
    def return_variable_value(argument, argument_metadata)
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        return_value = {}
        argument_metadata[:fields].each do |field, metadata|
          return_value[field] = @cursor[":#{argument}_o#{metadata[:position]}"]
        end
        return_value
      when 'PL/SQL BOOLEAN'
        numeric_value = @cursor[":o_#{argument}"]
        numeric_value.nil? ? nil : numeric_value == 1
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
    
    def dbms_output_sql
      if @dbms_output_stream
        dbms_output_enable_sql = "DBMS_OUTPUT.ENABLE(#{@schema.dbms_output_buffer_size});\n"
        # if database version is at least 10.2 then use DBMS_OUTPUT.GET_LINES with SYS.DBMSOUTPUT_LINESARRAY
        if (@schema.connection.database_version <=> [10, 2, 0, 0]) >= 0
          @declare_sql << "l_dbms_output_numlines INTEGER := #{Schema::DBMS_OUTPUT_MAX_LINES};\n"
          dbms_output_get_sql = "DBMS_OUTPUT.GET_LINES(:dbms_output_lines, l_dbms_output_numlines);\n"
          @bind_values[:dbms_output_lines] = nil
          @bind_metadata[:dbms_output_lines] = {:data_type => 'TABLE', :data_length => nil,
            :sql_type_name => "SYS.DBMSOUTPUT_LINESARRAY", :in_out => 'OUT'}
          # if database version is less than 10.2 then use individual DBMS_OUTPUT.GET_LINE calls
        else
          dbms_output_get_sql = ""
        end
        [dbms_output_enable_sql, dbms_output_get_sql]
      else
        ["", ""]
      end
    end

    def dbms_output_log
      if @dbms_output_stream
        # if database version is at least 10.2 then :dbms_output_lines output bind variable has dbms_output lines
        if @bind_metadata[:dbms_output_lines]
          @cursor[':dbms_output_lines'].each do |line|
            @dbms_output_stream.puts "DBMS_OUTPUT: #{line}" if line
          end
          # if database version is less than 10.2 then use individual DBMS_OUTPUT.GET_LINE calls
        else
          cursor = @schema.connection.parse("BEGIN sys.dbms_output.get_line(:line, :status); END;")
          while true do
            cursor.bind_param(':line', nil, :data_type => 'VARCHAR2', :in_out => 'OUT')
            cursor.bind_param(':status', nil, :data_type => 'NUMBER', :in_out => 'OUT')
            cursor.exec
            break unless cursor[':status'] == 0
            @dbms_output_stream.puts "DBMS_OUTPUT: #{cursor[':line']}"
          end
          cursor.close
        end
        @dbms_output_stream.flush
      end
    end
    
  end
  
  module PGProcedureCallHelper
    
    MATCHING_TYPES = {
      :integer => ['INTEGER', 'NUMERIC'],
      :decimal => ['NUMERIC'],
      :string => ['TEXT', 'CHARACTER VARYING', 'VARCHAR', 'CHAR'],
      :date => ['DATE'],
      :time => ['DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITHOUT TIME ZONE'],
      :boolean => ['BOOLEAN'],
      :hash => ['RECORD'],
      :array => ['ARRAY'],
      :cursor => ['CURSOR']
    }
    
    def matching_db_types_for_ruby_value(value)
      case value
      when NilClass
        :all
      when Fixnum, Bignum
        MATCHING_TYPES[:integer]
      when BigDecimal, Float
        MATCHING_TYPES[:decimal]
      when String
        MATCHING_TYPES[:string]
      when Date
        MATCHING_TYPES[:date]
      when Time
        MATCHING_TYPES[:time]
      when TrueClass, FalseClass
        MATCHING_TYPES[:boolean]
      when Hash
        MATCHING_TYPES[:hash]
      when Array
        MATCHING_TYPES[:array]
      when CursorCommon
        MATCHING_TYPES[:cursor]
      end
    end
    
    def exec
      @cursor = @schema.connection.parse(@sql)

      @bind_values.each do |arg, value|
        @cursor.bind_param(@bind_metadata[arg][:position], value, @bind_metadata[arg])
      end

      @return_vars.each do |var|
        @cursor.bind_param(@return_vars_metadata[var][:position], nil, @return_vars_metadata[var])
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
    end
    
    def construct_sql(args)
      @sql = ''
      @call_sql = ''
      @return_vars = []
      @return_vars_metadata = {}
      @params = []
      
      if return_metadata
        add_return
        @call_sql << '?= ' if defined?(JRuby)
        @params += ['$1::void'] unless defined?(JRuby)
      end
      
      @call_sql << (defined?(JRuby)? 'call ': 'SELECT * FROM ')
      @call_sql << "#{schema_name}." if schema_name
      @call_sql << "#{package_name}." if package_name
      @call_sql << "#{procedure_name}("
      
      @bind_values = {}
      @bind_metadata = {}

      if args.size == 1 && args[0].is_a?(Hash) && args[0].keys.all?{|k| k.is_a?(Symbol)} &&
          !(argument_list.size == 1 && (arguments[(only_argument = argument_list[0])][:data_type]) == 'RECORD' && args[0].keys != [only_argument])
        arguments.each do |arg, metadata|
          if !args[0].has_key?(arg) && metadata[:in_out] == 'OUT'
            args[0][arg] = nil
          end
        end
        @params += args[0].map do |arg, value|
          "#{arg} := " << add_argument(arg, value)
        end
      else
        argument_count = argument_list.size
        raise ArgumentError, 'Too many arguments passed to PL/SQL procedure' if args.size > argument_count
        if args.size < argument_count && (args.size...argument_count).all?{|i| arguments[argument_list[i]][:in_out] == 'OUT'}
          args += [nil] * (argument_count - args.size)
        end
        
        @params += args.each_with_index.map do |value, i|
          arg = argument_list[i]
          add_argument(arg, value)
        end
      end
      @call_sql << @params.join(', ')
      @call_sql << ')'
      @call_sql << ' AS return' unless defined?(JRuby)
      
      @sql << '{' if defined?(JRuby)
      @sql << @call_sql
      @sql << '}' if defined?(JRuby)
    end
    
    def add_argument(argument, value, argument_metadata = nil)
      argument_metadata ||= arguments[argument]
      raise ArgumentError, "Wrong argument #{argument.inspect} passed to PL/SQL procedure" unless argument_metadata
      case argument_metadata[:data_type]
      when 'RECORD'
        add_record(argument, value, argument_metadata)
      else
        @bind_values[argument] = value
        @bind_metadata[argument] = argument_metadata
        sql = defined?(JRuby)? '?': "$#{argument_metadata[:position] + 1}"
        sql << '::' << (argument_metadata[:in_out] == 'OUT'? 'void': (argument_metadata[:data_type]))
      end
    end
    
    def add_record(argument, record_value, argument_metadata)
      sql = 'ROW(' << (record_value||{}).map do |key, value|
        field = key.is_a?(Symbol) ? key : key.to_s.downcase.to_sym
        metadata = argument_metadata[:fields][field]
        raise ArgumentError, "Wrong field name #{key.inspect} passed to PL/SQL record argument #{argument.inspect}" unless metadata
        bind_variable = :"#{argument}_f#{metadata[:position]}"
        add_argument(bind_variable, value, metadata)
      end.join(', ')
      sql << ')'
    end
    
    def add_return_variable(argument, argument_metadata, is_return_value = false)
      @return_vars << argument
      @return_vars_metadata[argument] = argument_metadata
    end
    
    def return_variable_value(argument, argument_metadata)
      defined?(JRuby)? @cursor[argument_metadata[:position]]: @cursor[argument.to_s]
    end
    
  end
  
end