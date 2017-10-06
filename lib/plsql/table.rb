module PLSQL

  module TableClassMethods #:nodoc:
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
            WHERE s.owner = :owner
              AND s.synonym_name = :synonym_name
              AND t.owner = s.table_owner
              AND t.table_name = s.table_name
            UNION ALL
            SELECT t.owner, t.table_name
            FROM all_synonyms s, all_tables t
            WHERE s.owner = 'PUBLIC'
              AND s.synonym_name = :synonym_name
              AND t.owner = s.table_owner
              AND t.table_name = s.table_name",
            schema.schema_name, table.to_s.upcase, table.to_s.upcase))
        new(schema, row[1], row[0])
      else
        nil
      end
    end
  end

  class Table
    extend TableClassMethods

    attr_reader :columns, :schema_name, :table_name #:nodoc:

    def initialize(schema, table, override_schema_name = nil) #:nodoc:
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @table_name = table.to_s.upcase
      @columns = {}

      @schema.select_all(
        "SELECT c.column_name, c.column_id position,
              c.data_type, c.data_length, c.data_precision, c.data_scale, c.char_used,
              c.data_type_owner, c.data_type_mod,
              CASE WHEN c.data_type_owner IS NULL THEN NULL
              ELSE (SELECT t.typecode FROM all_types t
                WHERE t.owner = c.data_type_owner
                AND t.type_name = c.data_type) END typecode,
              c.nullable, c.data_default
        FROM all_tab_columns c
        WHERE c.owner = :owner
        AND c.table_name = :table_name",
        @schema_name, @table_name
      ) do |r|
        column_name, position,
              data_type, data_length, data_precision, data_scale, char_used,
              data_type_owner, _, typecode, nullable, data_default = r
        # remove scale (n) from data_type (returned for TIMESTAMPs and INTERVALs)
        data_type.sub!(/\(\d+\)/,'')
        # store column metadata
        @columns[column_name.downcase.to_sym] = {
          :position => position && position.to_i,
          :data_type => data_type_owner && (typecode == 'COLLECTION' ? 'TABLE' : 'OBJECT' ) || data_type,
          :data_length => data_type_owner ? nil : data_length && data_length.to_i,
          :data_precision => data_precision && data_precision.to_i,
          :data_scale => data_scale && data_scale.to_i,
          :char_used => char_used,
          :type_owner => data_type_owner,
          :type_name => data_type_owner && data_type,
          :sql_type_name => data_type_owner && "#{data_type_owner}.#{data_type}",
          :nullable => nullable == 'Y', # store as true or false
          :data_default => data_default && data_default.strip # remove leading and trailing whitespace
        }
      end
    end

    # list of table column names
    def column_names
      @column_names ||= @columns.keys.sort_by{|k| columns[k][:position]}
    end

    # General select method with :first, :all or :count as first parameter.
    # It is recommended to use #first, #all or #count method instead of this one.
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
        where_sqls = []
        order_by_sql = nil
        sql_params.each do |k,v|
          if k == :order_by
            order_by_sql = " ORDER BY #{v} "
          elsif v.nil? || v == :is_null
            where_sqls << "#{k} IS NULL"
          elsif v == :is_not_null
            where_sqls << "#{k} IS NOT NULL"
          else
            where_sqls << "#{k} = :#{k}"
            bindvars << v
          end
        end
        select_sql << "WHERE " << where_sqls.join(' AND ') unless where_sqls.empty?
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

    # Select all table records using optional conditions. Examples:
    #
    #   plsql.employees.all
    #   plsql.employees.all(:order_by => :employee_id)
    #   plsql.employees.all("WHERE employee_id > :employee_id", 5)
    # 
    def all(sql='', *bindvars)
      select(:all, sql, *bindvars)
    end

    # Select first table record using optional conditions. Examples:
    # 
    #   plsql.employees.first
    #   plsql.employees.first(:employee_id => 1)
    #   plsql.employees.first("WHERE employee_id = 1")
    #   plsql.employees.first("WHERE employee_id = :employee_id", 1)
    # 
    def first(sql='', *bindvars)
      select(:first, sql, *bindvars)
    end

    # Count table records using optional conditions. Examples:
    # 
    #   plsql.employees.count
    #   plsql.employees.count("WHERE employee_id > :employee_id", 5)
    # 
    def count(sql='', *bindvars)
      select(:count, sql, *bindvars)
    end

    # Insert record or records in table. Examples:
    # 
    #   employee = { :employee_id => 1, :first_name => 'First', :last_name => 'Last', :hire_date => Time.local(2000,01,31) }
    #   plsql.employees.insert employee
    #   # => INSERT INTO employees VALUES (1, 'First', 'Last', ...)
    # 
    #   employees = [employee1, employee2, ... ]  # array of many Hashes
    #   plsql.employees.insert employees
    #
    def insert(record)
      # if Array of records is passed then insert each individually
      if record.is_a?(Array)
        record.each {|r| insert(r)}
        return nil
      end

      table_proc = TableProcedure.new(@schema, self, :insert)
      table_proc.add_insert_arguments(record)

      call = ProcedureCall.new(table_proc, table_proc.argument_values)
      call.exec
    end

    # Insert record or records in table using array of values. Examples:
    # 
    #   # with values for all columns
    #   plsql.employees.insert_values [1, 'First', 'Last', Time.local(2000,01,31)]
    #   # => INSERT INTO employees VALUES (1, 'First', 'Last', ...)
    # 
    #   # with values for specified columns
    #   plsql.employees.insert_values [:employee_id, :first_name, :last_name], [1, 'First', 'Last']
    #   # => INSERT INTO employees (employee_id, first_name, last_name) VALUES (1, 'First', 'Last')
    # 
    #   # with values for many records
    #   plsql.employees.insert_values [:employee_id, :first_name, :last_name], [1, 'First', 'Last'], [2, 'Second', 'Last']
    #   # => INSERT INTO employees (employee_id, first_name, last_name) VALUES (1, 'First', 'Last')
    #   # => INSERT INTO employees (employee_id, first_name, last_name) VALUES (2, 'Second', 'Last')
    #
    def insert_values(*args)
      raise ArgumentError, "no arguments given" unless args.first
      # if first argument is array of symbols then use it as list of fields
      if args.first.all?{|a| a.instance_of?(Symbol)}
        fields = args.shift
      # otherwise use all columns as list of fields
      else
        fields = column_names
      end
      args.each do |record|
        raise ArgumentError, "record should be Array of values" unless record.is_a?(Array)
        raise ArgumentError, "wrong number of column values" unless record.size == fields.size
        insert(ArrayHelpers::to_hash(fields, record))
      end
    end

    # Update table records using optional conditions. Example:
    # 
    #   plsql.employees.update(:first_name => 'Second', :where => {:employee_id => 1})
    #   # => UPDATE employees SET first_name = 'Second' WHERE employee_id = 1
    #
    def update(params)
      raise ArgumentError, "Only Hash parameter can be passed to table update method" unless params.is_a?(Hash)
      where = params.delete(:where)
      
      table_proc = TableProcedure.new(@schema, self, :update)
      table_proc.add_set_arguments(params)
      table_proc.add_where_arguments(where) if where
      call = ProcedureCall.new(table_proc, table_proc.argument_values)
      call.exec
    end

    # Delete table records using optional conditions. Example:
    # 
    #   plsql.employees.delete(:employee_id => 1)
    #   # => DELETE FROM employees WHERE employee_id = 1
    # 
    def delete(sql_params='', *bindvars)
      delete_sql = "DELETE FROM \"#{@schema_name}\".\"#{@table_name}\" "
      case sql_params
      when String
        delete_sql << sql_params
      when Hash
        raise ArgumentError, "Cannot specify bind variables when passing WHERE conditions as Hash" unless bindvars.empty?
        where_sqls = []
        sql_params.each do |k,v|
          where_sqls << "#{k} = :#{k}"
          bindvars << v
        end
        delete_sql << "WHERE " << where_sqls.join(' AND ') unless where_sqls.empty?
      else
        raise ArgumentError, "Only String or Hash can be provided as SQL condition argument"
      end
      @schema.execute(delete_sql, *bindvars)
    end

    # wrapper class to simulate Procedure class for ProcedureClass#exec
    class TableProcedure #:nodoc:
      attr_reader :arguments, :argument_list, :return, :out_list, :schema

      def initialize(schema, table, operation)
        @schema = schema
        @table = table
        @operation = operation

        @return = [nil]
        @out_list = [[]]

        case @operation
        when :insert
          @argument_list = [[]]
          @arguments = [{}]
          @insert_columns = []
          @insert_values = []
        when :update
          @argument_list = [[]]
          @arguments = [{}]
          @set_sqls = []
          @set_values = []
          @where_sqls = []
          @where_values = []
        end
      end

      def overloaded?
        false
      end

      def procedure
        nil
      end

      def add_insert_arguments(params)
        params.each do |k,v|
          raise ArgumentError, "Invalid column name #{k.inspect} specified as argument" unless (column_metadata = @table.columns[k])
          @argument_list[0] << k
          @arguments[0][k] = column_metadata
          @insert_values << v
        end
      end

      def add_set_arguments(params)
        params.each do |k,v|
          raise ArgumentError, "Invalid column name #{k.inspect} specified as argument" unless (column_metadata = @table.columns[k])
          @argument_list[0] << k
          @arguments[0][k] = column_metadata
          @set_sqls << "#{k}=:#{k}"
          @set_values << v
        end
      end

      def add_where_arguments(params)
        case params
        when Hash
          params.each do |k,v|
            raise ArgumentError, "Invalid column name #{k.inspect} specified as argument" unless (column_metadata = @table.columns[k])
            @argument_list[0] << :"w_#{k}"
            @arguments[0][:"w_#{k}"] = column_metadata
            @where_sqls << "#{k}=:w_#{k}"
            @where_values << v
          end
        when String
          @where_sqls << params
        end
      end

      def argument_values
        case @operation
        when :insert
          @insert_values
        when :update
          @set_values + @where_values
        end
      end

      def call_sql(params_string)
        case @operation
        when :insert
          "INSERT INTO \"#{@table.schema_name}\".\"#{@table.table_name}\"(#{@argument_list[0].map{|a| a.to_s}.join(', ')}) VALUES (#{params_string});\n"
        when :update
          update_sql = "UPDATE \"#{@table.schema_name}\".\"#{@table.table_name}\" SET #{@set_sqls.join(', ')}"
          update_sql << " WHERE #{@where_sqls.join(' AND ')}" unless @where_sqls.empty?
          update_sql << ";\n"
          update_sql
        end
      end

    end

  end

end
