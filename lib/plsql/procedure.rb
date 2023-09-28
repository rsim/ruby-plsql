module PLSQL
  module ProcedureClassMethods #:nodoc:
    def find(schema, procedure, package = nil, override_schema_name = nil)
      if package.nil?
        if (row = schema.select_first(
          "SELECT #{procedure_object_id_src(schema)}.object_id
          FROM all_procedures p, all_objects o
          WHERE p.owner = :owner
            AND p.object_name = :object_name
            AND o.owner = p.owner
            AND o.object_name = p.object_name
            AND o.object_type in ('PROCEDURE', 'FUNCTION')",
            schema.schema_name, procedure.to_s.upcase))
          new(schema, procedure, nil, nil, row[0])
        # search for synonym
        elsif (row = schema.select_first(
          "SELECT o.owner, o.object_name, #{procedure_object_id_src(schema)}.object_id
          FROM all_synonyms s, all_objects o, all_procedures p
          WHERE s.owner IN (:owner, 'PUBLIC')
            AND s.synonym_name = :synonym_name
            AND o.owner = s.table_owner
            AND o.object_name = s.table_name
            AND o.object_type IN ('PROCEDURE','FUNCTION')
            AND o.owner = p.owner
            AND o.object_name = p.object_name
            ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)",
            schema.schema_name, procedure.to_s.upcase))
          new(schema, row[1], nil, row[0], row[2])
        else
          nil
        end
      elsif package && (row = schema.select_first(
        # older Oracle versions do not have object_id column in all_procedures
        "SELECT #{procedure_object_id_src(schema)}.object_id
        FROM all_procedures p, all_objects o
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
    end

    private

      def procedure_object_id_src(schema)
        (schema.connection.database_version <=> [11, 1, 0, 0]) >= 0 ? "p" : "o"
      end
  end

  module ProcedureCommon #:nodoc:
    attr_reader :arguments, :argument_list, :out_list, :return
    attr_reader :schema, :schema_name, :package, :procedure

    # return type string from metadata that can be used in DECLARE block or table definition
    def self.type_to_sql(metadata) #:nodoc:
      case metadata[:data_type]
      when "NUMBER"
        precision, scale = metadata[:data_precision], metadata[:data_scale]
        "NUMBER#{precision ? "(#{precision}#{scale ? ",#{scale}" : ""})" : ""}"
      when "VARCHAR", "VARCHAR2", "CHAR"
        length = case metadata[:char_used]
                 when "C" then "#{metadata[:char_length]} CHAR"
                 when "B" then "#{metadata[:data_length]} BYTE"
                 else
                   metadata[:data_length]
        end
        "#{metadata[:data_type]}#{length && "(#{length})"}"
      when "NVARCHAR2", "NCHAR"
        length = metadata[:char_length]
        "#{metadata[:data_type]}#{length && "(#{length})"}"
      when "PL/SQL TABLE", "TABLE", "VARRAY", "OBJECT", "XMLTYPE"
        metadata[:sql_type_name]
      else
        metadata[:data_type]
      end
    end

    # get procedure argument metadata from data dictionary
    def get_argument_metadata #:nodoc:
      if (@schema.connection.database_version <=> [18, 0, 0, 0]) >= 0
        get_argument_metadata_from_18c
      else
        get_argument_metadata_below_18c
      end
    end

    def get_argument_metadata_below_18c #:nodoc:
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
      subprogram_id_column = (@schema.connection.database_version <=> [10, 2, 0, 2]) >= 0 ? "subprogram_id" : "NULL"
      # defaulted is available just from version 11g
      defaulted_column = (@schema.connection.database_version <=> [11, 0, 0, 0]) >= 0 ? "defaulted" : "NULL"

      @schema.select_all(
        "SELECT #{subprogram_id_column}, object_name, TO_NUMBER(overload), argument_name, position, data_level,
              data_type, in_out, data_length, data_precision, data_scale, char_used,
              char_length, type_owner, type_name, type_subname, #{defaulted_column}
        FROM all_arguments
        WHERE object_id = :object_id
        AND owner = :owner
        AND object_name = :procedure_name
        ORDER BY overload, sequence",
        @object_id, @schema_name, @procedure
      ) do |r|

        subprogram_id, object_name, overload, argument_name, position, data_level,
            data_type, in_out, data_length, data_precision, data_scale, char_used,
            char_length, type_owner, type_name, type_subname, defaulted = r

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
          elsif data_type != "PL/SQL RECORD"
            # raise exception only when there are no overloaded procedure definitions
            # (as probably this overload will not be used at all)
            raise ArgumentError, "Parameter type #{sql_type_name} definition inside package is not supported, use CREATE TYPE outside package" if overload == 0
          end
        end

        argument_metadata = {
          position: position && position.to_i,
          data_type: data_type,
          in_out: in_out,
          data_length: data_length && data_length.to_i,
          data_precision: data_precision && data_precision.to_i,
          data_scale: data_scale && data_scale.to_i,
          char_used: char_used,
          char_length: char_used && char_length && char_length.to_i,
          type_owner: type_owner,
          type_name: type_name,
          type_subname: type_subname,
          sql_type_name: sql_type_name,
          defaulted: defaulted
        }
        if tmp_table_name
          @tmp_table_names[overload] << [(argument_metadata[:tmp_table_name] = tmp_table_name), argument_metadata]
        end

        if composite_type?(data_type)
          case data_type
          when "PL/SQL RECORD"
            argument_metadata[:fields] = {}
          end
          previous_level_argument_metadata[data_level] = argument_metadata
        end

        # if function has return value
        if argument_name.nil? && data_level == 0 && in_out == "OUT"
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
            when "PL/SQL RECORD"
              previous_level_argument_metadata[data_level - 1][:fields][argument_name.downcase.to_sym] = argument_metadata
            when "PL/SQL TABLE", "TABLE", "VARRAY", "REF CURSOR"
              previous_level_argument_metadata[data_level - 1][:element] = argument_metadata
            end
          end
        end
      end
      # if procedure is without arguments then create default empty argument list for default overload
      @arguments[0] = {} if @arguments.keys.empty?

      construct_argument_list_for_overloads
    end

    # get procedure argument metadata from data dictionary
    def get_argument_metadata_from_18c #:nodoc:
      @arguments = {}
      @argument_list = {}
      @out_list = {}
      @return = {}
      @overloaded = false

      # store tmp tables for each overload for table parameters with types defined inside packages
      @tmp_table_names = {}
      # store if tmp tables are created for specific overload
      @tmp_tables_created = {}

      @schema.select_all(
        "SELECT a.subprogram_id, a.object_name, TO_NUMBER(a.overload), a.argument_name, a.position,
          a.data_type, a.in_out, a.data_length, a.data_precision, a.data_scale, a.char_used,
          a.char_length, a.type_owner, nvl(a.type_subname, a.type_name) type_name,
          case when a.type_object_type = 'PACKAGE' then a.type_name end type_package, a.type_object_type, a.defaulted,
          s.table_owner synonym_owner, s.table_name synonym_name
        FROM all_arguments a
        LEFT JOIN all_synonyms s ON a.type_owner = s.owner AND a.type_name = s.synonym_name
        WHERE a.object_id = :object_id
          AND a.owner = :owner
          AND a.object_name = :procedure_name
        ORDER BY a.overload, a.sequence",
        @object_id, @schema_name, @procedure
      ) do |r|

        subprogram_id, _object_name, overload, argument_name, position,
          data_type, in_out, data_length, data_precision, data_scale, char_used,
          char_length, type_owner, type_name, type_package, type_object_type, defaulted,
          synonym_dest_owner, synonym_dest_name = r

        @overloaded ||= !overload.nil?
        # if not overloaded then store arguments at key 0
        overload ||= 0
        @arguments[overload] ||= {}
        @return[overload] ||= nil
        @tmp_table_names[overload] ||= []

        unless synonym_dest_owner.nil?
          @schema.select_all(
            "SELECT o.owner, o.object_name, o.object_type
            FROM all_objects o
            WHERE o.owner = :synonym_owner
              AND o.object_name = :synonym_name
              AND o.object_type IN ('PACKAGE', 'TYPE')",
            synonym_dest_owner, synonym_dest_name
          ) do |r2|
            tmp_owner, tmp_name, tmp_type = r2
            if tmp_type == 'PACKAGE'
              type_owner, type_package, type_object_type = tmp_owner, tmp_name, tmp_type
            else
              type_owner, type_name, type_object_type = tmp_owner, tmp_name, tmp_type
            end
          end
        end

        sql_type_name = build_sql_type_name(type_owner, type_package, type_name)

        tmp_table_name = nil
        # type defined inside package
        if type_package
          if collection_type?(data_type)
            tmp_table_name = "#{Connection::RUBY_TEMP_TABLE_PREFIX}#{@schema.connection.session_id}_#{@object_id}_#{subprogram_id}_#{position}"
          end
        end

        argument_metadata = {
          position: position && position.to_i,
          data_type: data_type,
          in_out: in_out,
          data_length: data_length && data_length.to_i,
          data_precision: data_precision && data_precision.to_i,
          data_scale: data_scale && data_scale.to_i,
          char_used: char_used,
          char_length: char_used && char_length && char_length.to_i,
          type_owner: type_owner,
          type_name: type_name,
          # TODO: should be renamed to type_package, when support for legacy database versions is dropped
          # due to the explicit change declaration of types in oracle plsql_type-catalogs (type_package + type_name),
          # the assignment of type + subtype was switched here for 18c and beyond
          type_subname: type_package,
          sql_type_name: sql_type_name,
          defaulted: defaulted,
          type_object_type: type_object_type
        }
        if tmp_table_name
          @tmp_table_names[overload] << [(argument_metadata[:tmp_table_name] = tmp_table_name), argument_metadata]
        end

        if composite_type?(data_type)
          case data_type
          when "PL/SQL RECORD", "REF CURSOR"
            argument_metadata[:fields] = get_field_definitions(argument_metadata)
          when "PL/SQL TABLE", "TABLE", "VARRAY"
            argument_metadata[:element] = get_element_definition(argument_metadata)
          end
        end

        # if function has return value
        if argument_name.nil? && in_out == "OUT"
          @return[overload] = argument_metadata
        else
          # sometime there are empty IN arguments in all_arguments view for procedures without arguments (e.g. for DBMS_OUTPUT.DISABLE)
          @arguments[overload][argument_name.downcase.to_sym] = argument_metadata if argument_name
        end
      end
      # if procedure is without arguments then create default empty argument list for default overload
      @arguments[0] = {} if @arguments.keys.empty?

      construct_argument_list_for_overloads
    end

    def construct_argument_list_for_overloads #:nodoc:
      @overloads = @arguments.keys.sort
      @overloads.each do |overload|
        @argument_list[overload] = @arguments[overload].keys.sort { |k1, k2| @arguments[overload][k1][:position] <=> @arguments[overload][k2][:position] }
        @out_list[overload] = @argument_list[overload].select { |k| @arguments[overload][k][:in_out] =~ /OUT/ }
      end
    end

    def ensure_tmp_tables_created(overload) #:nodoc:
      return if @tmp_tables_created.nil? || @tmp_tables_created[overload]
      @tmp_table_names[overload] && @tmp_table_names[overload].each do |table_name, argument_metadata|
        sql = "CREATE GLOBAL TEMPORARY TABLE #{table_name} (\n"
        element_metadata = argument_metadata[:element]
        case element_metadata[:data_type]
        when "PL/SQL RECORD"
          fields_metadata = element_metadata[:fields]
          fields_sorted_by_position = fields_metadata.keys.sort_by { |k| fields_metadata[k][:position] }
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

    def build_sql_type_name(type_owner, type_package, type_name) #:nodoc:
      if type_owner == nil || type_owner == "PUBLIC"
        type_owner_res = ""
      else
        type_owner_res = "#{type_owner}."
      end

      if type_package == nil
        type_name_res = type_name
      else
        type_name_res = "#{type_package}.#{type_name}"
      end
      type_name_res && "#{type_owner_res}#{type_name_res}"
    end

    def get_field_definitions(argument_metadata) #:nodoc:
      fields = {}
      case argument_metadata[:type_object_type]
      when "PACKAGE"
        @schema.select_all(
          "SELECT ta.attr_no, ta.attr_name,
            nvl(s.table_owner, attr_type_owner) attr_type_owner,
            CASE WHEN ta.attr_type_package IS NOT NULL THEN ta.attr_type_name ELSE nvl(s.table_name, ta.attr_type_name) END attr_type_name,
            CASE WHEN ta.attr_type_package IS NOT NULL THEN nvl(s.table_name, ta.attr_type_package) END attr_type_package,
            ta.length, ta.precision, ta.scale, ta.char_used
          FROM all_plsql_type_attrs ta
          LEFT JOIN all_synonyms s ON ta.attr_type_owner = s.owner AND nvl(ta.attr_type_package, ta.attr_type_name) = s.synonym_name
          WHERE ta.owner = :owner
            AND ta.type_name = :type_name
            AND ta.package_name = :package_name
          ORDER BY ta.attr_no",
          argument_metadata[:type_owner], argument_metadata[:type_name], argument_metadata[:type_subname]) do |r|

          attr_no, attr_name, attr_type_owner, attr_type_name, attr_type_package, attr_length, attr_precision, attr_scale, attr_char_used = r

          attr_type_name = 'PLS_INTEGER' if attr_type_name == 'PL/SQL PLS INTEGER'
          attr_type_name = 'BINARY_INTEGER' if attr_type_name == 'PL/SQL BINARY INTEGER'

          composite_type = nil
          if attr_type_owner != nil
            if attr_type_package != nil
              composite_type = get_composite_type(attr_type_owner, attr_type_name, attr_type_package)
            else
              composite_type = 'TABLE'
            end
          end
          fields[attr_name.downcase.to_sym] = {
            position: attr_no.to_i,
            data_type: attr_type_owner == nil ? attr_type_name : composite_type,
            in_out: argument_metadata[:in_out],
            data_length: attr_length && attr_length.to_i,
            data_precision: attr_precision && attr_precision.to_i,
            data_scale: attr_scale && attr_scale.to_i,
            char_used: attr_char_used == nil ? "0" : attr_char_used,
            char_length: attr_char_used && attr_length && attr_length.to_i,
            type_owner: attr_type_owner,
            type_name: attr_type_owner && attr_type_name,
            type_subname: attr_type_package,
            sql_type_name: attr_type_owner && build_sql_type_name(attr_type_owner, attr_type_package, attr_type_name),
            defaulted: argument_metadata[:defaulted],
            type_object_type: composite_type && composite_type == 'TABLE' ? 'TYPE' : nil
          }
          if composite_type == 'TABLE'
            fields[attr_name.downcase.to_sym][:element] = get_element_definition(fields[attr_name.downcase.to_sym])
          end

          if fields[attr_name.downcase.to_sym][:data_type] == "TABLE" && fields[attr_name.downcase.to_sym][:type_subname] != nil
            fields[attr_name.downcase.to_sym][:fields] = get_field_definitions(fields[attr_name.downcase.to_sym])
          end
        end
      when "TABLE", "VIEW"
        @schema.select_all(
          "SELECT column_id, column_name, data_type, data_length, data_precision, data_scale, char_length, char_used
           FROM ALL_TAB_COLS WHERE OWNER = :owner AND TABLE_NAME = :type_name
            AND hidden_column != 'YES'
           ORDER BY column_id",
           argument_metadata[:type_owner], argument_metadata[:type_name]) do |r|

          col_no, col_name, col_type_name, col_length, col_precision, col_scale, col_char_length, col_char_used = r

          if col_type_name.match('TIMESTAMP\(\d+\) WITH LOCAL TIME ZONE')
            col_type_name = 'TIMESTAMP WITH LOCAL TIME ZONE'
          end
          fields[col_name.downcase.to_sym] = {
            position: col_no.to_i,
            data_type: col_type_name,
            in_out: argument_metadata[:in_out],
            data_length: col_length && col_length.to_i,
            data_precision: col_precision && col_precision.to_i,
            data_scale: col_scale && col_scale.to_i,
            char_used: col_char_used == nil ? "0" : col_char_used,
            char_length: col_char_used && col_char_length && col_char_length.to_i,
            type_owner: nil,
            type_name: nil,
            type_subname: nil,
            sql_type_name: nil,
            defaulted: argument_metadata[:defaulted]
          }
        end
      end
      fields
    end

    def get_element_definition(argument_metadata) #:nodoc:
      element_metadata = {}
      if collection_type?(argument_metadata[:data_type])
        case argument_metadata[:type_object_type]
        when "PACKAGE"
          r = @schema.select_first(
            "SELECT nvl(s.table_owner, t.elem_type_owner) elem_type_owner,
              CASE WHEN t.elem_type_package IS NOT NULL THEN t.elem_type_name ELSE nvl(s.table_name, t.elem_type_name) END elem_type_name,
              CASE WHEN t.elem_type_package IS NOT NULL THEN nvl(s.table_name, t.elem_type_package) END elem_type_package_new,
              length, precision, scale, char_used, index_by
            FROM all_plsql_coll_types t
            LEFT JOIN all_synonyms s ON t.elem_type_owner = s.owner AND nvl(t.elem_type_package, t.elem_type_name) = s.synonym_name
            WHERE t.owner = :owner
              AND t.type_name = :type_name
              AND t.package_name = :package_name",
            argument_metadata[:type_owner], argument_metadata[:type_name], argument_metadata[:type_subname])

          elem_type_owner, elem_type_name, elem_type_package, elem_length, elem_precision, elem_scale, elem_char_used, index_by = r

          if index_by == "VARCHAR2"
            raise ArgumentError, "Index-by Varchar-Table (associative array) #{argument_metadata[:type_name]} is not supported"
          end

          if elem_type_name.match('%ROWTYPE')
            fields = {}
            @schema.select_all(
              "select column_id, column_name, data_type, data_length, data_precision, data_scale, char_used, char_length 
              FROM all_tab_columns 
              where owner = :owner and table_name = :table_name 
              order by column_id",
              elem_type_owner, elem_type_name.sub('%ROWTYPE', '')) do |r|
              
              rowtype_column_id, rowtype_column_name, rowtype_data_type, rowtype_data_length, rowtype_data_precision, rowtype_data_scale, rowtype_char_used, rowtype_char_length = r

              fields[rowtype_column_name.downcase.to_sym] = {
                position: rowtype_column_id.to_i,
                data_type: rowtype_data_type,
                in_out: 'OUT',
                data_length: rowtype_data_length && rowtype_data_length.to_i,
                data_precision: rowtype_data_precision && rowtype_data_precision.to_i,
                data_scale: rowtype_data_scale && rowtype_data_scale.to_i,
                char_used: rowtype_char_used == nil ? "0" : rowtype_char_used,
                char_length: rowtype_char_used && rowtype_char_length && rowtype_char_length.to_i,
                type_owner: nil,
                type_name: nil,
                type_subname: nil,
                sql_type_name: nil,
                defaulted: 'N'
              }
            end
            element_metadata = {
              position: 1,
              data_type: "PL/SQL RECORD",
              in_out: argument_metadata[:in_out],
              data_length: elem_length && elem_length.to_i,
              data_precision: elem_precision && elem_precision.to_i,
              data_scale: elem_scale && elem_scale.to_i,
              char_used: elem_char_used,
              char_length: elem_char_used && elem_length && elem_length.to_i,
              type_owner: elem_type_owner,
              type_name: elem_type_name,
              type_subname: elem_type_package,
              sql_type_name: elem_type_owner && build_sql_type_name(elem_type_owner, elem_type_package, elem_type_name),
              type_object_type: elem_type_package != nil ? "PACKAGE" : nil,
              defaulted: argument_metadata[:defaulted],
              fields: fields
            }
          else
            element_metadata = {
              position: 1,
              data_type: if elem_type_owner == nil
                          elem_type_name
                        elsif elem_type_package != nil
                          "PL/SQL RECORD"
                        else 
                          "OBJECT"
                        end,
              in_out: argument_metadata[:in_out],
              data_length: elem_length && elem_length.to_i,
              data_precision: elem_precision && elem_precision.to_i,
              data_scale: elem_scale && elem_scale.to_i,
              char_used: elem_char_used,
              char_length: elem_char_used && elem_length && elem_length.to_i,
              type_owner: elem_type_owner,
              type_name: elem_type_name,
              type_subname: elem_type_package,
              sql_type_name: elem_type_owner && build_sql_type_name(elem_type_owner, elem_type_package, elem_type_name),
              type_object_type: elem_type_package != nil ? "PACKAGE" : nil,
              defaulted: argument_metadata[:defaulted]
            }
          end

          if elem_type_package != nil
            element_metadata[:fields] = get_field_definitions(element_metadata)
          end
        when "TYPE"
          r = @schema.select_first(
            "SELECT nvl(s.table_owner, t.elem_type_owner), nvl(s.table_name, t.elem_type_name), t.length, t.precision, t.scale, t.char_used
            FROM all_coll_types t
            LEFT JOIN all_synonyms s ON t.elem_type_owner = s.owner AND t.elem_type_name = s.synonym_name
            WHERE t.owner = :owner
              AND t.type_name = :type_name",
            argument_metadata[:type_owner], argument_metadata[:type_name]
          )
          elem_type_owner, elem_type_name, elem_length, elem_precision, elem_scale, elem_char_used = r

          element_metadata = {
            position: 1,
            data_type: elem_type_owner == nil ? elem_type_name : "OBJECT",
            in_out: argument_metadata[:in_out],
            data_length: elem_length && elem_length.to_i,
            data_precision: elem_precision && elem_precision.to_i,
            data_scale: elem_scale && elem_scale.to_i,
            char_used: elem_char_used,
            char_length: elem_char_used && elem_length && elem_length.to_i,
            type_owner: elem_type_owner,
            type_name: elem_type_name,
            type_subname: nil,
            sql_type_name: elem_type_owner && build_sql_type_name(elem_type_owner, nil, elem_type_name),
            defaulted: argument_metadata[:defaulted]
          }
        end
      else
        element_metadata = {
          position: 1,
          data_type: "PL/SQL RECORD",
          in_out: argument_metadata[:in_out],
          data_length: nil,
          data_precision: nil,
          data_scale: nil,
          char_used: "B",
          char_length: 0,
          type_owner: argument_metadata[:type_owner],
          type_name: argument_metadata[:type_name],
          type_subname: argument_metadata[:type_subname],
          sql_type_name: build_sql_type_name(argument_metadata[:type_owner], argument_metadata[:type_subname], argument_metadata[:type_name]),
          defaulted: argument_metadata[:defaulted]
        }

        if element_metadata[:type_subname] != nil
          element_metadata[:fields] = get_field_definitions(element_metadata)
        end
      end
      element_metadata
    end

    def get_composite_type(type_owner, type_name, type_package)
      r = @schema.select_first("SELECT typecode FROM all_plsql_types WHERE owner = :owner AND type_name = :type_name AND package_name = :type_package
                                UNION ALL
                                SELECT typecode FROM all_types WHERE owner = :owner AND type_name = :type_name",
                                type_owner, type_name, type_package, type_owner, type_name)
      typecode = r[0]
      raise ArgumentError, "#{type_name} type #{build_sql_type_name(type_owner, type_package, type_name)} definition inside package is not supported as part of other type definition," <<
        " use CREATE TYPE outside package" if typecode == "COLLECTION"
      typecode
    end

    PLSQL_COMPOSITE_TYPES = ["PL/SQL RECORD", "PL/SQL TABLE", "TABLE", "VARRAY", "REF CURSOR"].freeze
    def composite_type?(data_type) #:nodoc:
      PLSQL_COMPOSITE_TYPES.include? data_type
    end

    PLSQL_COLLECTION_TYPES = ["PL/SQL TABLE", "TABLE", "VARRAY"].freeze
    def collection_type?(data_type) #:nodoc:
      PLSQL_COLLECTION_TYPES.include? data_type
    end

    def overloaded? #:nodoc:
      @overloaded
    end
  end

  class Procedure #:nodoc:
    extend ProcedureClassMethods
    include ProcedureCommon

    attr_reader :arguments, :argument_list, :out_list, :return
    attr_reader :schema, :schema_name, :package, :procedure

    def initialize(schema, procedure, package, override_schema_name, object_id)
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @procedure = procedure.to_s.upcase
      @package = package
      @object_id = object_id

      get_argument_metadata
    end

    def exec(*args, &block)
      call = ProcedureCall.new(self, args)
      call.exec(&block)
    end
  end
end
