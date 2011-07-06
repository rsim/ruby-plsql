module PLSQL
  
  module ProcedureHelperProvider
    
    def procedure_helper(dialect)
      case dialect
      when :oracle
        ORAProcedureHelper
      when :postgres
        PGProcedureHelper
      end
    end
    
  end
  
  module ProcedureHelper
    
    module ClassMethods
    
      def find(schema, procedure, package = nil, override_schema_name = nil)
        case schema.connection.dialect
        when :oracle
          if package.nil?
            if (row = schema.select_first(
                  "SELECT object_id FROM all_objects
                  WHERE owner = :owner
                  AND object_name = :object_name
                  AND object_type IN ('PROCEDURE','FUNCTION')",
                  schema.schema_name, procedure.to_s.upcase))
              new(schema, procedure, nil, nil, row[0])
            # search for synonym
            elsif (row = schema.select_first(
                  "SELECT o.owner, o.object_name, o.object_id
                  FROM all_synonyms s, all_objects o
                  WHERE s.owner IN (:owner, 'PUBLIC')
                  AND s.synonym_name = :synonym_name
                  AND o.owner = s.table_owner
                  AND o.object_name = s.table_name
                  AND o.object_type IN ('PROCEDURE','FUNCTION')
                  ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)",
                  schema.schema_name, procedure.to_s.upcase))
              new(schema, row[1], nil, row[0], row[2])
            else
              nil
            end
          elsif package && (row = schema.select_first(
                # older Oracle versions do not have object_id column in all_procedures
                "SELECT o.object_id FROM all_procedures p, all_objects o
                WHERE p.owner = :owner
                AND p.object_name = :object_name
                AND p.procedure_name = :procedure_name
                AND o.owner = p.owner
                AND o.object_name = p.object_name
                AND o.object_type = 'PACKAGE'",
                override_schema_name || schema.schema_name, package, procedure.to_s.upcase))
            new(schema, procedure, package, override_schema_name, row[0])
          else
            nil
          end
        when :postgres
          if (schema.select_first(
                "SELECT specific_name FROM information_schema.routines
                WHERE UPPER(routine_schema) = '#{override_schema_name || schema.schema_name}'
                AND UPPER(routine_name) = '#{procedure.to_s.upcase}'"))
            new(schema, procedure, nil, override_schema_name, nil)
          end
        end
      end
      
    end
    
    def self.included(host_class)
      host_class.extend(ClassMethods)
    end
    
  end
  
  module ORAProcedureHelper
    
    attr_reader :arguments, :argument_list, :out_list, :return
    attr_reader :schema, :schema_name, :package, :procedure

    # get procedure argument metadata from data dictionary
    def get_argument_metadata #:nodoc:
      @arguments = {}
      @argument_list = {}
      @out_list = {}
      @return = {}
      @overloaded = false

      # store reference to previous level record or collection metadata
      previous_level_argument_metadata = {}

      # store tmp tables for each overload for table parameters with types defined inside packages
      @tmp_table_names = {}
      # store if tmp tables are created for specific overload
      @tmp_tables_created = {}

      # subprogram_id column is available just from version 10g
      subprogram_id_column = (@schema.connection.database_version <=> [10, 2, 0, 2]) >= 0 ? 'subprogram_id' : 'NULL'

      @schema.select_all(
        "SELECT #{subprogram_id_column}, object_name, TO_NUMBER(overload), argument_name, position, data_level,
        data_type, in_out, data_length, data_precision, data_scale, char_used,
        char_length, type_owner, type_name, type_subname
        FROM all_arguments
        WHERE object_id = :object_id
        AND owner = :owner
        AND object_name = :procedure_name
        ORDER BY overload, sequence",
        @object_id, @schema_name, @procedure
      ) do |r|

        subprogram_id, object_name, overload, argument_name, position, data_level,
          data_type, in_out, data_length, data_precision, data_scale, char_used,
          char_length, type_owner, type_name, type_subname = r

        @overloaded ||= !overload.nil?
        # if not overloaded then store arguments at key 0
        overload ||= 0
        @arguments[overload] ||= {}
        @return[overload] ||= nil
        @tmp_table_names[overload] ||= []

        sql_type_name = type_owner && "#{type_owner == 'PUBLIC' ? nil : "#{type_owner}."}#{type_name}#{type_subname ? ".#{type_subname}" : nil}"

        tmp_table_name = nil
        # type defined inside package
        if type_subname
          if collection_type?(data_type)
            raise ArgumentError, "#{data_type} type #{sql_type_name} definition inside package is not supported as part of other type definition," <<
              " use CREATE TYPE outside package" if data_level > 0
            # if subprogram_id was not supported by all_arguments view
            # then generate unique ID from object_name and overload
            subprogram_id ||= "#{object_name.hash % 10000}#{overload}"
            tmp_table_name = "#{Connection::RUBY_TEMP_TABLE_PREFIX}#{@schema.connection.session_id}_#{@object_id}_#{subprogram_id}_#{position}"
          elsif data_type != 'PL/SQL RECORD'
            # raise exception only when there are no overloaded procedure definitions
            # (as probably this overload will not be used at all)
            raise ArgumentError, "Parameter type #{sql_type_name} definition inside package is not supported, use CREATE TYPE outside package" if overload == 0
          end
        end

        argument_metadata = {
          :position => position && position.to_i,
          :data_type => data_type,
          :in_out => in_out,
          :data_length => data_length && data_length.to_i,
          :data_precision => data_precision && data_precision.to_i,
          :data_scale => data_scale && data_scale.to_i,
          :char_used => char_used,
          :char_length => char_length && char_length.to_i,
          :type_owner => type_owner,
          :type_name => type_name,
          :type_subname => type_subname,
          :sql_type_name => sql_type_name
        }
        if tmp_table_name
          @tmp_table_names[overload] << [(argument_metadata[:tmp_table_name] = tmp_table_name), argument_metadata]
        end

        if composite_type?(data_type)
          case data_type
          when 'PL/SQL RECORD'
            argument_metadata[:fields] = {}
          end
          previous_level_argument_metadata[data_level] = argument_metadata
        end

        # if function has return value
        if argument_name.nil? && data_level == 0 && in_out == 'OUT'
          @return[overload] = argument_metadata
          # if parameter
        else
          # top level parameter
          if data_level == 0
            # sometime there are empty IN arguments in all_arguments view for procedures without arguments (e.g. for DBMS_OUTPUT.DISABLE)
            @arguments[overload][argument_name.downcase.to_sym] = argument_metadata if argument_name
            # or lower level part of composite type
          else
            case previous_level_argument_metadata[data_level - 1][:data_type]
            when 'PL/SQL RECORD'
              previous_level_argument_metadata[data_level - 1][:fields][argument_name.downcase.to_sym] = argument_metadata
            when 'PL/SQL TABLE', 'TABLE', 'VARRAY', 'REF CURSOR'
              previous_level_argument_metadata[data_level - 1][:element] = argument_metadata
            end
          end
        end
      end
      # if procedure is without arguments then create default empty argument list for default overload
      @arguments[0] = {} if @arguments.keys.empty?

      construct_argument_list_for_overloads
    end

    def construct_argument_list_for_overloads #:nodoc:
      @overloads = @arguments.keys.sort
      @overloads.each do |overload|
        @argument_list[overload] = @arguments[overload].keys.sort {|k1, k2| @arguments[overload][k1][:position] <=> @arguments[overload][k2][:position]}
        @out_list[overload] = @argument_list[overload].select {|k| @arguments[overload][k][:in_out] =~ /OUT/}
      end
    end

    def ensure_tmp_tables_created(overload) #:nodoc:
      return if @tmp_tables_created.nil? || @tmp_tables_created[overload]
      @tmp_table_names[overload] && @tmp_table_names[overload].each do |table_name, argument_metadata|
        sql = "CREATE GLOBAL TEMPORARY TABLE #{table_name} (\n"
        element_metadata = argument_metadata[:element]
        case element_metadata[:data_type]
        when 'PL/SQL RECORD'
          fields_metadata = element_metadata[:fields]
          fields_sorted_by_position = fields_metadata.keys.sort_by{|k| fields_metadata[k][:position]}
          sql << fields_sorted_by_position.map do |field|
            metadata = fields_metadata[field]
            "#{field} #{ProcedureCommon.type_to_sql(metadata)}"
          end.join(",\n")
        else
          sql << "element #{ProcedureCommon.type_to_sql(element_metadata)}"
        end
        sql << ",\ni__ NUMBER(38)\n"
        sql << ") ON COMMIT PRESERVE ROWS\n"
        sql_block = "DECLARE\nPRAGMA AUTONOMOUS_TRANSACTION;\nBEGIN\nEXECUTE IMMEDIATE :sql;\nEND;\n"
        @schema.execute sql_block, sql
      end
      @tmp_tables_created[overload] = true
    end

    PLSQL_COMPOSITE_TYPES = ['PL/SQL RECORD', 'PL/SQL TABLE', 'TABLE', 'VARRAY', 'REF CURSOR'].freeze
    def composite_type?(data_type) #:nodoc:
      PLSQL_COMPOSITE_TYPES.include? data_type
    end

    PLSQL_COLLECTION_TYPES = ['PL/SQL TABLE', 'TABLE', 'VARRAY'].freeze
    def collection_type?(data_type) #:nodoc:
      PLSQL_COLLECTION_TYPES.include? data_type
    end

    def overloaded? #:nodoc:
      @overloaded
    end
    
  end
  
  module PGProcedureHelper
    
    attr_reader :arguments, :argument_list, :out_list, :return
    attr_reader :schema, :schema_name, :package, :procedure
    
    # Function definitions adapted from: http://www.alberton.info/postgresql_meta_info.html
    def function_args
      sql = <<-SQL
      
        CREATE OR REPLACE FUNCTION datatype(
          IN typename character varying,
          IN schema character varying)
        RETURNS character varying AS $$
          SELECT types.data_type FROM (SELECT n.nspname AS type_schema, t.typname AS type_name,
          CASE WHEN t.typrelid = 0 THEN 'ENUM' ELSE 'RECORD' END data_type
          FROM pg_catalog.pg_type t
          LEFT JOIN pg_catalog.pg_namespace n
          ON (n.oid = t.typnamespace)
          WHERE (t.typrelid = 0 OR 
            (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
          AND NOT EXISTS
            (SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          UNION ALL
          SELECT pg_tables.schemaname AS type_schema, pg_tables.tablename AS type_name,
          'RECORD' data_type
          FROM pg_catalog.pg_tables) types
          WHERE upper(types.type_name) = $1
          AND upper(types.type_schema) = $2
          AND pg_type_is_visible((types.type_schema || '.' || types.type_name)::regtype);
        $$ LANGUAGE 'sql' STABLE STRICT SECURITY INVOKER;



        CREATE OR REPLACE FUNCTION subtypes(
          IN typename character varying,
          IN schema character varying,
          INOUT datalevel integer,
          OUT pos integer,
          OUT subtype_name character varying,
          OUT subtype_datatype character varying,
          OUT subtype_typename character varying)
        RETURNS SETOF record AS $$
        DECLARE
          tmp_datalevel integer;
          tmp_position integer;
          subtype_info record;
          nested_subtype_info record;
          tmp_typename character varying;
        BEGIN
          tmp_datalevel := datalevel;
          tmp_position := 0;
          FOR subtype_info IN
          (SELECT upper(attributes.attribute_name) AS name,
          CASE WHEN attributes.data_type = 'USER-DEFINED'
            THEN upper(attributes.attribute_udt_name)
            ELSE upper(attributes.data_type)
          END typename
          FROM information_schema.attributes
          WHERE upper(attributes.udt_name) = typename
          AND upper(attributes.udt_schema) = schema
          ORDER BY attributes.ordinal_position)
          UNION ALL
          (SELECT upper(columns.column_name) AS name,
          upper(columns.data_type) AS typename
          FROM information_schema.columns
          WHERE upper(columns.table_name) = typename
          AND upper(columns.table_schema) = schema
          ORDER BY columns.ordinal_position)
          LOOP
            tmp_position := tmp_position + 1;
            pos := tmp_position;
            subtype_name := subtype_info.name;
            tmp_typename := subtype_info.typename;
            subtype_datatype := datatype(tmp_typename, schema);

            /* if subtype-datatype is complex type */
            IF subtype_datatype IS NOT NULL THEN
              subtype_typename := tmp_typename;
              RETURN NEXT;

              /* call subtypes recursively for nested subtypes */
              FOR nested_subtype_info IN SELECT * FROM subtypes(subtype_typename, schema, datalevel + 1) LOOP
                datalevel := nested_subtype_info.datalevel;
                pos := nested_subtype_info.pos;
                subtype_name := nested_subtype_info.subtype_name;
                subtype_datatype := nested_subtype_info.subtype_datatype;
                subtype_typename := nested_subtype_info.subtype_typename;
                RETURN NEXT;
                datalevel := tmp_datalevel;
              END LOOP;

            ELSE
              subtype_datatype := tmp_typename;
              RETURN NEXT;
            END IF;
          END LOOP;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql' STABLE STRICT SECURITY INVOKER;



        CREATE OR REPLACE FUNCTION function_args(
          IN funcname character varying,
          IN schema character varying,
          OUT overload integer,
          OUT datalevel integer,
          OUT pos integer,
          OUT direction character varying,
          OUT argname character varying,
          OUT datatype character varying,
          OUT typeowner character varying,
          OUT typename character varying)
        RETURNS SETOF record AS $$

        DECLARE
          procids oid[];
          rettype character varying;
          argtypes oidvector;
          allargtypes oid[];
          argmodes "char"[];
          argnames text[];

          typeid oid;
          tmp_typename character varying;
          subtype_info record;

          argidx integer;
          mini integer;
          maxi integer;

        BEGIN
          
          /* get all function ids with the same name */
          SELECT array(SELECT pg_proc.oid
          FROM pg_catalog.pg_proc
          JOIN pg_catalog.pg_namespace
          ON (pg_proc.pronamespace = pg_namespace.oid)
          WHERE pg_catalog.pg_function_is_visible(pg_proc.oid)
          AND upper(pg_proc.proname) = funcname
          AND upper(pg_namespace.nspname) = schema
          ORDER BY pg_proc.oid) INTO procids;

          /* exit if function not found */
          IF array_dims(procids) IS NULL THEN
            RETURN;
          END IF;

          /* for each function definition with the same name do */
          FOR i IN array_lower(procids, 1) .. array_upper(procids, 1) LOOP
          
            /* select argument type information into arrays */
            SELECT INTO rettype, argtypes, allargtypes, argmodes, argnames
            CASE WHEN pg_proc.proretset THEN 'SETOF ' || pg_catalog.format_type(pg_proc.prorettype, NULL)
            ELSE pg_catalog.format_type(pg_proc.prorettype, NULL) END,
            pg_proc.proargtypes,
            pg_proc.proallargtypes,
            pg_proc.proargmodes,
            pg_proc.proargnames
            FROM pg_catalog.pg_proc
            WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype
            AND (pg_proc.proargtypes[0] IS NULL OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)
            AND NOT pg_proc.proisagg
            AND pg_proc.oid  = procids[i];

            IF array_upper(procids, 1) > 1 THEN
              overload := i;
            END IF;

            pos := -1;

            /* return a row for the return value if there are no OUT parameters */
            IF allargtypes IS NULL THEN
              pos := 0;
              datalevel := 0;
              direction := 'OUT';
              argname := NULL;
              datatype := upper(rettype);
              typename := NULL;
              RETURN NEXT;
            END IF;

            /* allargtypes is NULL if there are no OUT parameters */
            IF allargtypes IS NULL THEN
              mini = array_lower(argtypes, 1);
              maxi = array_upper(argtypes, 1);
            ELSE
              mini = array_lower(allargtypes, 1);
              maxi = array_upper(allargtypes, 1);
            END IF;
            IF maxi < mini THEN RETURN; END IF;

            /* for each argument do */
            FOR j IN mini .. maxi LOOP
              pos = pos + 1;
              argidx = j - mini + 1;

              IF argnames IS NULL THEN
                argname = NULL;
              ELSE
                argname = argnames[argidx];
              END IF;

              IF allargtypes IS NULL THEN
                direction = 'IN';
                typeid = argtypes[j];
              ELSE
                direction = CASE WHEN argmodes[j] = 'i' THEN 'IN'
                  WHEN argmodes[j] = 'o' THEN 'OUT'
                  WHEN argmodes[j] = 'b' THEN 'IN/OUT' END;
                typeid = allargtypes[j];
              END IF;

              datalevel := 0;
              tmp_typename := upper(pg_catalog.format_type(typeid, NULL));
              datatype := datatype(tmp_typename, schema);
              typename := NULL;

              /* if argument datatype is complex type */
              IF datatype IS NOT NULL THEN
                typename := tmp_typename;

                RETURN NEXT;
                
                /* loop over all subtypes and return record for each */
                FOR subtype_info IN SELECT * from subtypes(typename, schema, 1) LOOP
                  datalevel := subtype_info.datalevel;
                  pos := subtype_info.pos;
                  argname := subtype_info.subtype_name;
                  datatype := subtype_info.subtype_datatype;
                  typename := subtype_info.subtype_typename;
                  RETURN NEXT;
                END LOOP;

              ELSE
                datatype := tmp_typename;
                RETURN NEXT;
              END IF;

            END LOOP;

          END LOOP;

          RETURN;
        END;$$ LANGUAGE 'plpgsql' STABLE STRICT SECURITY INVOKER;

        COMMENT ON FUNCTION function_args(character varying, character varying)
        IS $$For a function identifier and schema, this procedure selects for each
        argument the following data:
        - the overload-identifier if the function is overloaded (NULL if it isn't)
        - datalevel nested level of subtype (0 if top-level type)
        - position in the argument list (0 for the return value)
        - direction 'IN', 'OUT', or 'IN/OUT'
        - name (NULL if not defined)
        - data type
        - owner of the data type
        - name of complex type (NULL if simple-type)$$;
      SQL
      
      @schema.execute sql
      
    end
    
    def get_argument_metadata #:nodoc:
      @arguments = {}
      @argument_list = {}
      @out_list = {}
      @return = {}
      @overloaded = false
      
      # store reference to previous level record or collection metadata
      previous_level_argument_metadata = {}
      
      @schema.select_all("SELECT (function_args('#{@procedure}', '#{@schema_name}')).*") do |r|
      
        overload, data_level, position, in_out, argument_name, data_type, type_owner, type_name = r
        
        data_level ||= 0
        
        @overloaded ||= !overload.nil?
        
        # if not overloaded then store arguments at key 0
        overload ||= 0
        @arguments[overload] ||= {}
        @return[overload] ||= nil
        
        sql_type_name = type_owner && "#{type_owner == 'PUBLIC' ? nil : "#{type_owner}."}#{type_name}"
        
        argument_metadata = {
          :position => position && position.to_i,
          :data_type => data_type,
          :in_out => in_out,
          :type_owner => type_owner,
          :type_name => type_name,
          :type_subname => nil,
          :sql_type_name => sql_type_name
        }
        
        if composite_type?(data_type)
          case data_type
          when 'RECORD'
            argument_metadata[:fields] = {}
          end
          previous_level_argument_metadata[data_level] = argument_metadata
        end

        # if function has return value
        if argument_name.nil? && data_level == 0 && in_out == 'OUT'
          @return[overload] = argument_metadata
          # if parameter
        else
          # top level parameter
          if data_level == 0
            # sometime there are empty IN arguments in all_arguments view for procedures without arguments (e.g. for DBMS_OUTPUT.DISABLE)
            @arguments[overload][argument_name.downcase.to_sym] = argument_metadata if argument_name
            # or lower level part of composite type
          else
            case previous_level_argument_metadata[data_level - 1][:data_type]
            when 'RECORD'
              previous_level_argument_metadata[data_level - 1][:fields][argument_name.downcase.to_sym] = argument_metadata
            when 'ARRAY'
              previous_level_argument_metadata[data_level - 1][:element] = argument_metadata
            end
          end
        end
      end
    
      construct_argument_list_for_overloads
    end
    
    def construct_argument_list_for_overloads #:nodoc:
      @overloads = @arguments.keys.sort
      @overloads.each do |overload|
        @argument_list[overload] = @arguments[overload].keys.sort {|k1, k2| @arguments[overload][k1][:position] <=> @arguments[overload][k2][:position]}
        @out_list[overload] = @argument_list[overload].select {|k| @arguments[overload][k][:in_out] =~ /OUT/}
      end
    end
    
    PLSQL_COMPOSITE_TYPES = ['RECORD', 'ARRAY'].freeze
    def composite_type?(data_type) #:nodoc:
      PLSQL_COMPOSITE_TYPES.include? data_type
    end

    PLSQL_COLLECTION_TYPES = ['ARRAY'].freeze
    def collection_type?(data_type) #:nodoc:
      PLSQL_COLLECTION_TYPES.include? data_type
    end

    def overloaded? #:nodoc:
      @overloaded
    end
    
  end
  
end