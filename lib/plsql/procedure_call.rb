require "plsql/procedure_call_helpers"

module PLSQL
  
  class ProcedureCall #:nodoc:
    
    include ProcedureCallHelperProvider

    def initialize(procedure, args = [], options = {})
      @procedure = procedure
      @schema = @procedure.schema
      extend procedure_call_helper(@schema.connection.dialect)
      @dbms_output_stream = @schema.dbms_output_stream
      @skip_self = options[:skip_self]
      @self = options[:self]
      @overload = get_overload_from_arguments_list(args)
      @procedure.ensure_tmp_tables_created(@overload) if @procedure.respond_to?(:ensure_tmp_tables_created)
      construct_sql(args)
    end

    private
    
    def get_overload_from_arguments_list(args)
      # if not overloaded then overload index 0 is used
      return 0 unless @procedure.overloaded?
      # If named arguments are used then
      # there should be just one Hash argument with symbol keys
      if args.size == 1 && args[0].is_a?(Hash) && args[0].keys.all?{|k| k.is_a?(Symbol)}
        args_keys = args[0].keys
        # implicit SELF argument for object instance procedures
        args_keys << :self if @self && !args_keys.include?(:self)
        number_of_args = args_keys.size
        matching_overloads = [] # overloads with exact or smaller number of matching named arguments
        overload_argument_list.keys.each do |ov|
          # assume that missing arguments have default value
          missing_arguments_count = overload_argument_list[ov].size - number_of_args
          if missing_arguments_count >= 0 &&
              args_keys.all?{|k| overload_argument_list[ov].include?(k)}
            matching_overloads << [ov, missing_arguments_count]
          end
        end
        # pick first matching overload with smallest missing arguments count
        # (hoping that missing arguments will be defaulted - cannot find default value from all_arguments)
        overload = matching_overloads.sort_by{|ov, score| score}[0][0]
        # otherwise try matching by sequential arguments count and types
      else
        number_of_args = args.size
        matching_types = []
        # if implicit SELF argument for object instance procedures should be passed
        # then it should be added as first argument to find matches
        if @self
          number_of_args += 1
          matching_types << ['OBJECT']
        end
        args.each do |arg|
          matching_types << matching_db_types_for_ruby_value(arg)
        end
        exact_overloads = [] # overloads with exact number of matching arguments
        smaller_overloads = [] # overloads with smaller number of matching arguments
        # overload = overload_argument_list.keys.detect do |ov|
        #   overload_argument_list[ov].size == number_of_args
        # end
        overload_argument_list.keys.each do |ov|
          score = 0 # lower score is better match
          ov_arg_list_size = overload_argument_list[ov].size
          if (number_of_args <= ov_arg_list_size &&
                (0..(number_of_args-1)).all? do |i|
                ov_arg = overload_argument_list[ov][i]
                matching_types[i] == :all || # either value matches any type
                (ind = matching_types[i].index(overload_arguments[ov][ov_arg][:data_type])) &&
                  (score += ind) # or add index of matched type
              end)
            if number_of_args == ov_arg_list_size
              exact_overloads << [ov, score]
            else
              smaller_overloads << [ov, score]
            end
          end
        end
        # pick either first exact matching overload of first matching with smaller argument count
        # (hoping that missing arguments will be defaulted - cannot find default value from all_arguments)
        overload = if !exact_overloads.empty?
          exact_overloads.sort_by{|ov, score| score}[0][0]
        elsif !smaller_overloads.empty?
          smaller_overloads.sort_by{|ov, score| score}[0][0]
        end
      end
      raise ArgumentError, "Wrong number or types of arguments passed to overloaded PL/SQL procedure" unless overload
      overload
    end

    def record_fields_sorted_by_position(fields_metadata)
      fields_metadata.keys.sort_by{|k| fields_metadata[k][:position]}
    end

    def add_return
      add_return_variable(:return, return_metadata, true)
    end

    def add_out_variables
      out_list.each do |argument|
        add_return_variable(argument, arguments[argument])
      end
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

    def overload_argument_list
      @overload_argument_list ||=
        @skip_self ? @procedure.argument_list_without_self : @procedure.argument_list
    end

    def overload_arguments
      @overload_arguments ||=
        @skip_self ? @procedure.arguments_without_self : @procedure.arguments
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
      @out_list ||=
        @skip_self ? @procedure.out_list_without_self[@overload] : @procedure.out_list[@overload]
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