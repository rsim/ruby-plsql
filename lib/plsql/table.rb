module PLSQL

  module TableClassMethods
    def find(schema, table)
      if schema.select_first(
            "SELECT table_name FROM all_tables
            WHERE owner = :owner
              AND table_name = :table_name",
            schema.schema_name, table.to_s.upcase)
        new(schema, table)
      # search for synonym
      elsif (row = schema.select_first(
            "SELECT t.owner, t.table_name
            FROM all_synonyms s, all_tables t
            WHERE s.owner IN (:owner, 'PUBLIC')
              AND s.synonym_name = :synonym_name
              AND t.owner = s.table_owner
              AND t.table_name = s.table_name
            ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)",
            schema.schema_name, table.to_s.upcase))
        new(schema, row[1], row[0])
      else
        nil
      end
    end
  end

  class Table
    extend TableClassMethods

    attr_reader :columns, :schema_name, :table_name

    def initialize(schema, table, override_schema_name = nil)
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @table_name = table.to_s.upcase
      @columns = {}

      @schema.connection.select_all("
        SELECT column_name, column_id position,
              data_type, data_length, data_precision, data_scale, char_used,
              data_type_owner, data_type_mod
        FROM all_tab_columns
        WHERE owner = :owner
        AND table_name = :table_name
        ORDER BY column_id",
        @schema_name, @table_name
      ) do |r|
        column_name, position,
              data_type, data_length, data_precision, data_scale, char_used,
              data_type_owner, data_type_mod = r
        @columns[column_name.downcase.to_sym] = {
          :position => position && position.to_i,
          :data_type => data_type_owner && 'OBJECT' || data_type,
          :data_length => data_type_owner ? nil : data_length && data_length.to_i,
          :data_precision => data_precision && data_precision.to_i,
          :data_scale => data_scale && data_scale.to_i,
          :char_used => char_used,
          :type_owner => data_type_owner,
          :type_name => data_type_owner && data_type,
          :sql_type_name => data_type_owner && "#{data_type_owner}.#{data_type}"
        }
      end
    end

    def select(first_or_all, sql_params='', *bindvars)
      case first_or_all
      when :first, :all
        select_sql = "SELECT * "
      when :count
        select_sql = "SELECT COUNT(*) "
      else
        raise ArgumentError, "Only :first, :all or :count are supported"
      end
      select_sql << "FROM \"#{@schema_name}\".\"#{@table_name}\" "
      case sql_params
      when String
        select_sql << sql_params
      when Hash
        raise ArgumentError, "Cannot specify bind variables when passing WHERE conditions as Hash" unless bindvars.empty?
        where_sql = []
        order_by_sql = nil
        sql_params.each do |k,v|
          if k == :order_by
            order_by_sql = "ORDER BY #{v} "
          else
            where_sql << "#{k} = :#{k}"
            bindvars << v
          end
        end
        select_sql << "WHERE " << where_sql.join(' AND ') unless where_sql.blank?
        select_sql << order_by_sql if order_by_sql
      else
        raise ArgumentError, "Only String or Hash can be provided as SQL condition argument"
      end
      if first_or_all == :count
        @schema.select_one(select_sql, *bindvars)
      else
        @schema.select(first_or_all, select_sql, *bindvars)
      end
    end

    def all(sql='', *bindvars)
      select(:all, sql, *bindvars)
    end

    def first(sql='', *bindvars)
      select(:first, sql, *bindvars)
    end

    def count(sql='', *bindvars)
      select(:count, sql, *bindvars)
    end

    def insert(record)
      # if Array of records is passed then insert each individually
      if record.is_a?(Array)
        record.each {|r| insert(r)}
        return nil
      end

      call = ProcedureCall.new(TableProcedure.new(@schema, self, :insert), [record])
      call.exec
    end

    # wrapper class to simulate Procedure class for ProcedureClass#exec
    class TableProcedure
      attr_reader :arguments, :argument_list, :return, :out_list, :schema

      def initialize(schema, table, operation)
        @schema = schema
        @table = table
        @operation = operation

        @return = [nil]
        @out_list = [[]]

        @argument_list = [[:p_record]]
        @arguments = [{:p_record => {
          :data_type => 'PL/SQL RECORD',
          :fields => @table.columns
        }}]
      end

      def overloaded?
        false
      end

      def procedure
        nil
      end

      

      def call_sql(params_string)
        case @operation
        when :insert
          "INSERT INTO \"#{@table.schema_name}\".\"#{@table.table_name}\" VALUES #{params_string};\n"
        end
      end

    end

  end

end
