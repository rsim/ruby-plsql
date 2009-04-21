module PLSQL

  module ProcedureClassMethods
    def find(schema, procedure, package = nil, override_schema_name = nil)
      if package.nil?
        if schema.select_first("
            SELECT object_name FROM all_objects
            WHERE owner = :owner
              AND object_name = :object_name
              AND object_type IN ('PROCEDURE','FUNCTION')",
            schema.schema_name, procedure.to_s.upcase)
          new(schema, procedure)
        # search for synonym
        elsif (row = schema.select_first("
            SELECT o.owner, o.object_name
            FROM all_synonyms s, all_objects o
            WHERE s.owner IN (:owner, 'PUBLIC')
              AND s.synonym_name = :synonym_name
              AND o.owner = s.table_owner
              AND o.object_name = s.table_name
              AND o.object_type IN ('PROCEDURE','FUNCTION')
              ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)",
            schema.schema_name, procedure.to_s.upcase))
          new(schema, row[1], nil, row[0])
        else
          nil
        end
      elsif package && schema.select_first("
            SELECT object_name FROM all_procedures
            WHERE owner = :owner
              AND object_name = :object_name
              AND procedure_name = :procedure_name
          ", override_schema_name || schema.schema_name, package, procedure.to_s.upcase)
        new(schema, procedure, package, override_schema_name)
      else
        nil
      end
    end
  end

  class Procedure
    extend ProcedureClassMethods

    def initialize(schema, procedure, package = nil, override_schema_name = nil)
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @procedure = procedure.to_s.upcase
      @package = package
      @arguments = {}
      @argument_list = {}
      @out_list = {}
      @return = {}
      @overloaded = false
      # RSI: due to 10gR2 all_arguments performance issue SELECT split into two statements
      # added condition to ensure that if object is package then package specification not body is selected
      object_id = @schema.connection.select_first("
        SELECT o.object_id
        FROM all_objects o
        WHERE o.owner = :owner
        AND o.object_name = :object_name
        AND o.object_type <> 'PACKAGE BODY'
        ", @schema_name, @package ? @package : @procedure
      )[0] rescue nil
      num_rows = @schema.connection.select_all("
        SELECT a.argument_name, a.position, a.data_type, a.in_out, a.data_length, a.data_precision, a.data_scale, a.overload
        FROM all_arguments a
        WHERE a.object_id = :object_id
        AND a.owner = :owner
        AND a.object_name = :procedure_name
        AND NVL(a.package_name,'nil') = :package
        ", object_id, @schema_name, @procedure, @package ? @package : 'nil'
      ) do |r|

        argument_name, position, data_type, in_out, data_length, data_precision, data_scale, overload = r

        @overloaded ||= !overload.nil?
        # if not overloaded then store arguments at key 0
        overload ||= 0
        @arguments[overload] ||= {}
        @return[overload] ||= nil
        
        if argument_name
          @arguments[overload][argument_name.downcase.to_sym] = {
            :position => position,
            :data_type => data_type,
            :in_out => in_out,
            :data_length => data_length,
            :data_precision => data_precision,
            :data_scale => data_scale
          }
        # if function has return value
        elsif position == 0 && in_out == 'OUT'
          @return[overload] = {
            :data_type => data_type,
            :in_out => in_out,
            :data_length => data_length,
            :data_precision => data_precision,
            :data_scale => data_scale
          }
        end
      end
      # if procedure is without arguments then create default empty argument list for default overload
      @arguments[0] = {} if @arguments.keys.empty?
      
      @overloads = @arguments.keys.sort
      @overloads.each do |overload|
        @argument_list[overload] = @arguments[overload].keys.sort {|k1, k2| @arguments[overload][k1][:position] <=> @arguments[overload][k2][:position]}
        @out_list[overload] = @argument_list[overload].select {|k| @arguments[overload][k][:in_out] =~ /OUT/}
      end
    end
    
    def overloaded?
      @overloaded
    end

    def exec(*args)
      # find which overloaded definition to use
      # if definition is overloaded then match by number of arguments
      if @overloaded
        # named arguments
        if args.size == 1 && args[0].is_a?(Hash)
          number_of_args = args[0].keys.size
          overload = @argument_list.keys.detect do |ov|
            @argument_list[ov].size == number_of_args &&
            @arguments[ov].keys.sort_by{|k| k.to_s} == args[0].keys.sort_by{|k| k.to_s}
          end
        # sequential arguments
        # TODO: should try to implement matching by types of arguments
        else
          number_of_args = args.size
          overload = @argument_list.keys.detect do |ov|
            @argument_list[ov].size == number_of_args
          end
        end
        raise ArgumentError, "Wrong number of arguments passed to overloaded PL/SQL procedure" unless overload
      else
        overload = 0
      end

      sql = "BEGIN\n"
      sql << ":return := " if @return[overload]
      sql << "#{@schema_name}." if @schema_name
      sql << "#{@package}." if @package
      sql << "#{@procedure}("

      # Named arguments
      args_list = []
      args_hash = {}
      if args.size == 1 and args[0].is_a?(Hash)
        sql << args[0].map do |k,v|
          raise ArgumentError, "Wrong argument passed to PL/SQL procedure" unless @arguments[overload][k]
          args_list << k
          args_hash[k] = v
          "#{k.to_s} => :#{k.to_s}"
        end.join(', ')
      # Sequential arguments
      else
        raise ArgumentError, "Too many arguments passed to PL/SQL procedure" if args.size > @argument_list[overload].size
        # Add missing arguments with nil value
        args = args + [nil]*(@argument_list[overload].size-args.size) if args.size < @argument_list[overload].size
        i = 0
        sql << args.map do |v|
          k = @argument_list[overload][i]
          i += 1
          args_list << k
          args_hash[k] = v
          ":#{k.to_s}"
        end.join(', ')
      end
      sql << ");\n"
      sql << "END;\n"

      cursor = @schema.connection.parse(sql)
      
      args_list.each do |k|
        data_type, data_length = plsql_to_ruby_data_type(@arguments[overload][k])
        cursor.bind_param(":#{k.to_s}", ruby_value_to_ora_value(args_hash[k], data_type),
                                        data_type, data_length, @arguments[overload][k][:in_out])
      end
      
      if @return[overload]
        data_type, data_length = plsql_to_ruby_data_type(@return[overload])
        cursor.bind_param(":return", nil, data_type, data_length, 'OUT')
      end
      
      cursor.exec

      # if function with output parameters
      if @return[overload] && @out_list[overload].size > 0
        result = [ora_value_to_ruby_value(cursor[':return']), {}]
        @out_list[overload].each do |k|
          result[1][k] = ora_value_to_ruby_value(cursor[":#{k}"])
        end
      # if function without output parameters
      elsif @return[overload]
        result = ora_value_to_ruby_value(cursor[':return'])
      # if procedure with output parameters
      elsif @out_list[overload].size > 0
        result = {}
        @out_list[overload].each do |k|
          result[k] = ora_value_to_ruby_value(cursor[":#{k}"])
        end
      # if procedure without output parameters
      else
        result = nil
      end
      cursor.close
      result
    end
    
    private
    
    def plsql_to_ruby_data_type(argument)
      @schema.connection.plsql_to_ruby_data_type(argument[:data_type],argument[:data_length])
    end
    
    def ruby_value_to_ora_value(val, type)
      @schema.connection.ruby_value_to_ora_value(val, type)
    end
    
    def ora_value_to_ruby_value(val)
      @schema.connection.ora_value_to_ruby_value(val)
    end

  end

end