module PLSQL
  class Connection
    attr_reader :raw_driver
    attr_reader :activerecord_class

    def initialize(raw_conn, ar_class = nil) #:nodoc:
      @raw_driver = self.class.driver_type
      @raw_connection = raw_conn
      @activerecord_class = ar_class
    end

    def self.create(raw_conn, ar_class = nil) #:nodoc:
      if ar_class && !(defined?(::ActiveRecord) && ar_class.ancestors.include?(::ActiveRecord::Base))
        raise ArgumentError, "Wrong ActiveRecord class"
      end
      case driver_type
      when :oci
        OCIConnection.new(raw_conn, ar_class)
      when :jdbc
        JDBCConnection.new(raw_conn, ar_class)
      else
        raise ArgumentError, "Unknown raw driver"
      end
    end

    def self.create_new(params) #:nodoc:
      conn = case driver_type
      when :oci
        OCIConnection.create_raw(params)
      when :jdbc
        JDBCConnection.create_raw(params)
      else
        raise ArgumentError, "Unknown raw driver"
      end
      conn.set_time_zone(params[:time_zone]||ENV['ORA_SDTZ'])
      conn
    end

    def self.driver_type #:nodoc:
      # MRI 1.8.6 or YARV 1.9.1
      @driver_type ||= if (!defined?(RUBY_ENGINE) || RUBY_ENGINE == "ruby") && defined?(OCI8)
        :oci
      # JRuby
      elsif (defined?(RUBY_ENGINE) && RUBY_ENGINE == "jruby")
        :jdbc
      else
        nil
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

    # Is it OCI8 connection
    def oci?
      @raw_driver == :oci
    end

    # Is it JDBC connection
    def jdbc?
      @raw_driver == :jdbc
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

    # all_synonyms view is quite slow therefore
    # this implementation is overriden in OCI connection with faster native OCI method
    def describe_synonym(schema_name, synonym_name) #:nodoc:
      select_first(
      "SELECT table_owner, table_name FROM all_synonyms WHERE owner = :owner AND synonym_name = :synonym_name",
        schema_name.to_s.upcase, synonym_name.to_s.upcase)
    end

    # Returns array with major and minor version of database (e.g. [10, 2])
    def database_version
      raise NoMethodError, "Not implemented for this raw driver"
    end

    # Returns session ID
    def session_id
      @session_id ||= select_first("SELECT TO_NUMBER(USERENV('SESSIONID')) FROM dual")[0]
    end

    # Set time zone
    def set_time_zone(time_zone=nil)
      exec("alter session set time_zone = '#{time_zone}'") if time_zone
    end

    # Returns session time zone
    def time_zone
      select_first("SELECT SESSIONTIMEZONE FROM dual")[0]
    end

    RUBY_TEMP_TABLE_PREFIX = 'ruby_'

    # Drop all ruby temporary tables that are used for calling packages with table parameter types defined in packages
    def drop_all_ruby_temporary_tables
      select_all("SELECT table_name FROM user_tables WHERE temporary='Y' AND table_name LIKE :table_name",
                  RUBY_TEMP_TABLE_PREFIX.upcase+'%').each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end

    # Drop ruby temporary tables created in current session that are used for calling packages with table parameter types defined in packages
    def drop_session_ruby_temporary_tables
      select_all("SELECT table_name FROM user_tables WHERE temporary='Y' AND table_name LIKE :table_name",
                  RUBY_TEMP_TABLE_PREFIX.upcase+"#{session_id}_%").each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end

  end

end