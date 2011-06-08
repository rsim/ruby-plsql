module PLSQL
  
  class Connection
    
    RUBY_TEMP_TABLE_PREFIX = 'ruby_'
    
    attr_reader :activerecord_class
    attr_reader :dialect

    def initialize(raw_conn, params = {}) #:nodoc:
      params.reverse_merge!(:dialect => :oracle)
      @activerecord_class = params[:ar_class]
      @dialect = params[:dialect]
      @raw_connection = raw_conn
    end

    def self.create(raw_conn, params = {}) #:nodoc:
      params.reverse_merge!(:dialect => :oracle)
      if params[:ar_class] && !(defined?(::ActiveRecord) && [params[:ar_class], params[:ar_class].superclass].include?(::ActiveRecord::Base))
        raise ArgumentError, "Wrong ActiveRecord class"
      end
      driver = driver_type(params[:dialect])
      raise ArgumentError, "Unknown raw driver" unless driver
      driver.new(raw_conn, params)
    end

    def self.create_new(params, dialect = :oracle) #:nodoc:
      driver = driver_type(dialect)
      raise ArgumentError, "Unknown raw driver" unless driver
      conn = driver.create_raw(params)
      conn.set_time_zone(params[:time_zone])
      conn
    end

    def self.driver_type(dialect) #:nodoc:
      # MRI 1.8.6 or YARV 1.9.1
      if (!defined?(JRuby))
        case dialect
        when :oracle
          OCIConnection
        when :postgres
          PGConnection
        end
      # JRuby
      else
        case dialect
        when :oracle
          JDBCORAConnection
        when :postgres
          JDBCPGConnection
        end
      end
    end

    # Returns OCI8 or JDBC connection
    def raw_connection
      if @activerecord_class
        @activerecord_class.connection.raw_connection
      else
        @raw_connection
      end
    end
    
    def logoff #:nodoc:
      # Rollback any uncommited transactions
      rollback
      # Common cleanup activities before logoff, should be called from particular driver method
      drop_session_ruby_temporary_tables
    end

    def commit #:nodoc:
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def rollback #:nodoc:
      raise NoMethodError, "Not implemented for this raw driver"
    end

    # Current autocommit mode (true or false)
    def autocommit?
      raise NoMethodError, "Not implemented for this raw driver"
    end

    # Set autocommit mode (true or false)
    def autocommit=(value)
      raise NoMethodError, "Not implemented for this raw driver"
    end

    # Set number of rows to be prefetched. This can reduce the number of network round trips when fetching many rows.
    # The default value is one. (If ActiveRecord oracle_enhanced connection is used then default is 100)
    def prefetch_rows=(value)
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def select_first(sql, *bindvars) #:nodoc:
      cursor = cursor_from_query(sql, bindvars, :prefetch_rows => 1)
      cursor.fetch
    ensure
      cursor.close rescue nil
    end

    def select_hash_first(sql, *bindvars) #:nodoc:
      cursor = cursor_from_query(sql, bindvars, :prefetch_rows => 1)
      cursor.fetch_hash
    ensure
      cursor.close rescue nil
    end

    def select_all(sql, *bindvars, &block) #:nodoc:
      cursor = cursor_from_query(sql, bindvars)
      results = []
      row_count = 0
      while row = cursor.fetch
        if block_given?
          yield(row)
          row_count += 1
        else
          results << row
        end
      end
      block_given? ? row_count : results
    ensure
      cursor.close rescue nil
    end

    def select_hash_all(sql, *bindvars, &block) #:nodoc:
      cursor = cursor_from_query(sql, bindvars)
      results = []
      row_count = 0
      while row = cursor.fetch_hash
        if block_given?
          yield(row)
          row_count += 1
        else
          results << row
        end
      end
      block_given? ? row_count : results
    ensure
      cursor.close rescue nil
    end

    def exec(sql, *bindvars) #:nodoc:
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def parse(sql) #:nodoc:
      raise NoMethodError, "Not implemented for this raw driver"
    end

    module CursorCommon
      # Fetch all rows from cursor, each row as array of values
      def fetch_all
        rows = []
        while (row = fetch)
          rows << row
        end
        rows
      end

      # Fetch all rows from cursor, each row as hash {:column => value, ...}
      def fetch_hash_all
        rows = []
        while (row = fetch_hash)
          rows << row
        end
        rows
      end

      # Fetch row from cursor as hash {:column => value, ...}
      def fetch_hash
        (row = fetch) && ArrayHelpers::to_hash(fields, row)
      end
    end

    # Describe the given synonym by querying the given schema.
    def describe_synonym(schema_name, synonym_name) #:nodoc:
      raise NoMethodError, "Not implemented for this raw driver"
    end

    # Returns array with major and minor version of database (e.g. [10, 2])
    def database_version
      raise NoMethodError, "Not implemented for this raw driver"
    end
    
  end

end