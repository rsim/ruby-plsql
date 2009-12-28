module PLSQL
  class ProcedureCall #:nodoc:

    def initialize(procedure, args = [])
      @procedure = procedure
      @overload = get_overload_from_arguments_list(args)
      construct_sql(args)
    end

    def exec
      @cursor = @procedure.schema.connection.parse(@sql)

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
    end

    private

    def get_overload_from_arguments_list(args)
      # find which overloaded definition to use
      # if definition is overloaded then match by number of arguments
      if @procedure.overloaded?
        # named arguments
        if args.size == 1 && args[0].is_a?(Hash)
          number_of_args = args[0].keys.size
          overload = overload_argument_list.keys.detect do |ov|
            overload_argument_list[ov].size == number_of_args &&
            overload_arguments[ov].keys.sort_by{|k| k.to_s} == args[0].keys.sort_by{|k| k.to_s}
          end
        # sequential arguments
        # TODO: should try to implement matching by types of arguments
        else
          number_of_args = args.size
          overload = overload_argument_list.keys.detect do |ov|
            overload_argument_list[ov].size == number_of_args
          end
        end
        raise ArgumentError, "Wrong number of arguments passed to overloaded PL/SQL procedure" unless overload
        overload
      else
        0
      end
    end

    def construct_sql(args)
      @declare_sql = "DECLARE\n"
      @assignment_sql = "BEGIN\n"
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
      if args.size == 1 && args[0].is_a?(Hash) &&
            # do not use named arguments if procedure has just one PL/SQL record or object type argument -
            # in that case passed Hash should be used as value for this PL/SQL record argument
            # (which will be processed in sequential arguments bracnh)
            !(argument_list.size == 1 &&
              ['PL/SQL RECORD','OBJECT'].include?(arguments[(only_argument=argument_list[0])][:data_type]) &&
              args[0].keys != [only_argument])
        # Add missing output arguments with nil value
        arguments.each do |arg, metadata|
          if !args[0].has_key?(arg) && metadata[:in_out] == 'OUT'
            args[0][arg] = nil
          end
        end
        # Add passed parameters to procedure call with parameter names
        @call_sql << args[0].map do |arg, value|
          "#{arg} => " << add_argument(arg, value)
        end.join(', ')

      # Sequential arguments
      else
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
        @call_sql = @procedure.call_sql(@call_sql)
      end
      add_out_vars
      @sql = "" << @declare_sql << @assignment_sql << @call_sql << @return_sql << "END;\n"
    end

    def add_argument(argument, value)
      argument_metadata = arguments[argument]
      raise ArgumentError, "Wrong argument #{argument.inspect} passed to PL/SQL procedure" unless argument_metadata
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        @declare_sql << record_declaration_sql(argument, argument_metadata)
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
        @bind_values[argument] = value
        @bind_metadata[argument] = argument_metadata
        ":#{argument}"
      end
    end

    def record_declaration_sql(argument, argument_metadata)
      fields_metadata = argument_metadata[:fields]
      sql = "TYPE t_#{argument} IS RECORD (\n"
      fields_sorted_by_position = fields_metadata.keys.sort_by{|k| fields_metadata[k][:position]}
      sql << fields_sorted_by_position.map do |field|
        metadata = fields_metadata[field]
        "#{field} #{type_to_sql(metadata)}"
      end.join(",\n")
      sql << ");\n"
      sql << "l_#{argument} t_#{argument};\n"
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

    def add_return
      case return_metadata[:data_type]
      when 'PL/SQL RECORD'
        @declare_sql << record_declaration_sql('return', return_metadata)
        return_metadata[:fields].each do |field, metadata|
          bind_variable = :"return_f#{metadata[:position]}"
          @return_vars << bind_variable
          @return_vars_metadata[bind_variable] = metadata
          @return_sql << ":#{bind_variable} := l_return.#{field};\n"
        end
        "l_return := "
      when 'PL/SQL BOOLEAN'
        @declare_sql << "l_return BOOLEAN;\n"
        @declare_sql << "x_return NUMBER(1);\n"
        @return_vars << :return
        @return_vars_metadata[:return] = return_metadata.merge(:data_type => "NUMBER", :data_precision => 1)
        @return_sql << "IF l_return IS NULL THEN\nx_return := NULL;\nELSIF l_return THEN\nx_return := 1;\nELSE\nx_return := 0;\nEND IF;\n" <<
                        ":return := x_return;\n"
        "l_return := "
      else
        @return_vars << :return
        @return_vars_metadata[:return] = return_metadata
        ':return := '
      end
    end

    def add_out_vars
      out_list.each do |argument|
        argument_metadata = arguments[argument]
        case argument_metadata[:data_type]
        when 'PL/SQL RECORD'
          argument_metadata[:fields].each do |field, metadata|
            bind_variable = :"#{argument}_o#{metadata[:position]}"
            @return_vars << bind_variable
            @return_vars_metadata[bind_variable] = metadata
            @return_sql << ":#{bind_variable} := l_#{argument}.#{field};\n"
          end
        when 'PL/SQL BOOLEAN'
          @declare_sql << "x_#{argument} NUMBER(1);\n"
          bind_variable = :"o_#{argument}"
          @return_vars << bind_variable
          @return_vars_metadata[bind_variable] = argument_metadata.merge(:data_type => "NUMBER", :data_precision => 1)
          @return_sql << "IF l_#{argument} IS NULL THEN\nx_#{argument} := NULL;\n" <<
                        "ELSIF l_#{argument} THEN\nx_#{argument} := 1;\nELSE\nx_#{argument} := 0;\nEND IF;\n" <<
                        ":#{bind_variable} := x_#{argument};\n"
        end
      end
    end

    def type_to_sql(metadata)
      case metadata[:data_type]
      when 'NUMBER'
        precision, scale = metadata[:data_precision], metadata[:data_scale]
        "NUMBER#{precision ? "(#{precision}#{scale ? ",#{scale}": ""})" : ""}"
      when 'VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR'
        length = metadata[:data_length]
        if length && (char_used = metadata[:char_used])
          length = "#{length} #{char_used == 'C' ? 'CHAR' : 'BYTE'}"
        end
        "#{metadata[:data_type]}#{length ? "(#{length})": ""}"
      when 'TABLE', 'VARRAY', 'OBJECT'
        metadata[:sql_type_name]
      else
        metadata[:data_type]
      end
    end

    def get_return_value
      # if function with output parameters
      if return_metadata && out_list.size > 0
        result = [function_return_value, {}]
        out_list.each do |k|
          result[1][k] = out_var_value(k)
        end
      # if function without output parameters
      elsif return_metadata
        result = function_return_value
      # if procedure with output parameters
      elsif out_list.size > 0
        result = {}
        out_list.each do |k|
          result[k] = out_var_value(k)
        end
      # if procedure without output parameters
      else
        result = nil
      end
      result
    end

    def function_return_value
      case return_metadata[:data_type]
      when 'PL/SQL RECORD'
        return_value = {}
        return_metadata[:fields].each do |field, metadata|
          bind_variable = :"return_f#{metadata[:position]}"
          return_value[field] = @cursor[":#{bind_variable}"]
        end
        return_value
      when 'PL/SQL BOOLEAN'
        numeric_value = @cursor[':return']
        numeric_value.nil? ? nil : numeric_value == 1
      else
        @cursor[':return']
      end
    end

    def out_var_value(argument)
      argument_metadata = arguments[argument]
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        return_value = {}
        argument_metadata[:fields].each do |field, metadata|
          bind_variable = :"#{argument}_o#{metadata[:position]}"
          return_value[field] = @cursor[":#{bind_variable}"]
        end
        return_value
      when 'PL/SQL BOOLEAN'
        numeric_value = @cursor[":o_#{argument}"]
        numeric_value.nil? ? nil : numeric_value == 1
      else
        @cursor[":#{argument}"]
      end
    end

    def overload_argument_list
      @overload_argument_list ||= @procedure.argument_list
    end

    def overload_arguments
      @overload_arguments ||= @procedure.arguments
    end

    def argument_list
      @argument_list ||= overload_argument_list[@overload]
    end

    def arguments
      @arguments ||= overload_arguments[@overload]
    end

    def return_metadata
      @return_metadata ||= @procedure.return[@overload]
    end

    def out_list
      @out_list ||= @procedure.out_list[@overload]
    end

    def schema_name
      @schema_name ||= @procedure.schema_name
    end

    def package_name
      @package_name ||= @procedure.package
    end

    def procedure_name
      @procedure_name ||= @procedure.procedure
    end

  end

end