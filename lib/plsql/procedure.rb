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

      # store reference to previous level record or collection metadata
      previous_level_argument_metadata = {}

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
        SELECT a.argument_name, a.position, a.sequence, a.data_level,
              a.data_type, a.in_out, a.data_length, a.data_precision, a.data_scale, a.char_used, a.overload
        FROM all_arguments a
        WHERE a.object_id = :object_id
        AND a.owner = :owner
        AND a.object_name = :procedure_name
        AND NVL(a.package_name,'nil') = :package
        ORDER BY a.overload, a.sequence
        ", object_id, @schema_name, @procedure, @package ? @package : 'nil'
      ) do |r|

        argument_name, position, sequence, data_level,
            data_type, in_out, data_length, data_precision, data_scale, char_used, overload = r

        @overloaded ||= !overload.nil?
        # if not overloaded then store arguments at key 0
        overload ||= 0
        @arguments[overload] ||= {}
        @return[overload] ||= nil
        
        argument_metadata = {
          :position => position,
          :data_type => data_type,
          :in_out => in_out,
          :data_length => data_length,
          :data_precision => data_precision,
          :data_scale => data_scale,
          :char_used => char_used
        }
        if composite_type?(data_type)
          case data_type
          when 'PL/SQL RECORD'
            argument_metadata[:fields] = {}
          end
          previous_level_argument_metadata[data_level] = argument_metadata
        end

        # if parameter
        if argument_name
          # top level parameter
          if data_level == 0
            @arguments[overload][argument_name.downcase.to_sym] = argument_metadata
          # or lower level part of composite type
          else
            case previous_level_argument_metadata[data_level - 1][:data_type]
            when 'PL/SQL RECORD'
              previous_level_argument_metadata[data_level - 1][:fields][argument_name.downcase.to_sym] = argument_metadata
            end
          end
        # if function has return value
        elsif argument_name.nil? && data_level == 0 && in_out == 'OUT'
          @return[overload] = argument_metadata
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

    PLSQL_COMPOSITE_TYPES = ['PL/SQL RECORD', 'TABLE', 'OBJECT', 'REF CURSOR'].freeze
    def composite_type?(data_type)
      PLSQL_COMPOSITE_TYPES.include? data_type
    end

    def overloaded?
      @overloaded
    end

    def exec(*args)
      overload = get_overload_from_arguments_list(args)

      declare_sql = "DECLARE\n"
      assignment_sql = "BEGIN\n"
      call_sql = ""
      call_sql << ":return := " if @return[overload]
      call_sql << "#{@schema_name}." if @schema_name
      call_sql << "#{@package}." if @package
      call_sql << "#{@procedure}("

      bind_values = {}
      bind_metadata = {}

      # Named arguments
      if args.size == 1 and args[0].is_a?(Hash)
        call_sql << args[0].map do |arg, value|
          argument_metadata = @arguments[overload][arg]
          raise ArgumentError, "Wrong argument passed to PL/SQL procedure" unless argument_metadata
          case argument_metadata[:data_type]
          when 'PL/SQL RECORD'
            declare_sql << record_declaration_sql(arg, argument_metadata)
            record_assignment_sql, record_bind_values, record_bind_metadata =
              record_assignment_sql_values_metadata(arg, argument_metadata, value)
            assignment_sql << record_assignment_sql
            bind_values.merge!(record_bind_values)
            bind_metadata.merge!(record_bind_metadata)
            "#{arg} => l_#{arg}"
          else
            # args_list << k
            bind_values[arg] = value
            bind_metadata[arg] = argument_metadata
            "#{arg} => :#{arg}"
          end
        end.join(', ')

      # Sequential arguments
      else
        argument_count = @argument_list[overload].size
        raise ArgumentError, "Too many arguments passed to PL/SQL procedure" if args.size > argument_count
        # Add missing arguments with nil value
        args += [nil] * (argument_count - args.size) if args.size < argument_count
        call_sql << (0...args.size).map do |i|
          arg = @argument_list[overload][i]
          value = args[i]
          argument_metadata = @arguments[overload][arg]
          bind_values[arg] = value
          bind_metadata[arg] = argument_metadata
          ":#{arg}"
        end.join(', ')
      end

      call_sql << ");\n"
      sql_block = "" << declare_sql << assignment_sql << call_sql << "END;\n"
      # puts "DEBUG: sql_block = #{sql_block.gsub "\n", "<br/>\n"}"
      cursor = @schema.connection.parse(sql_block)
      
      bind_values.each do |arg, value|
        cursor.bind_param(":#{arg}", value, bind_metadata[arg])
      end

      if @return[overload]
        cursor.bind_param(":return", nil, @return[overload])
      end
      
      cursor.exec

      # if function with output parameters
      if @return[overload] && @out_list[overload].size > 0
        result = [cursor[':return'], {}]
        @out_list[overload].each do |k|
          result[1][k] = cursor[":#{k}"]
        end
      # if function without output parameters
      elsif @return[overload]
        result = cursor[':return']
      # if procedure with output parameters
      elsif @out_list[overload].size > 0
        result = {}
        @out_list[overload].each do |k|
          result[k] = cursor[":#{k}"]
        end
      # if procedure without output parameters
      else
        result = nil
      end
      result
    ensure
      cursor.close if defined?(cursor) && cursor
    end
    
    private

    def get_overload_from_arguments_list(args)
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
        overload
      else
        0
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
      argument_metadata[:fields].each do |field, metadata|
        if value = record_value[field] || record_value[field.to_s]
          bind_variable = :"#{argument}_#{field}"
          sql << "l_#{argument}.#{field} := :#{bind_variable};\n"
          bind_values[bind_variable] = value
          bind_metadata[bind_variable] = metadata
        end
      end
      [sql, bind_values, bind_metadata]
    end

    def type_to_sql(metadata)
      case metadata[:data_type]
      when 'NUMBER'
        precision, scale = metadata[:data_precision], metadata[:data_scale]
        "NUMBER#{precision ? "(#{precision.to_i}#{scale ? ",#{scale.to_i}": ""})" : ""}"
      when 'VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR'
        if length = metadata[:data_length]
          length = length.to_i
        end
        if length && (char_used = metadata[:char_used])
          length = "#{length} #{char_used == 'C' ? 'CHAR' : 'BYTE'}"
        end
        "#{metadata[:data_type]}#{length ? "(#{length})": ""}"
      else
        metadata[:data_type]
      end
    end

  end

end