module PLSQL
  class Connection
    attr_reader :raw_driver
    attr_reader :activerecord_class

    def initialize(raw_drv, raw_conn, ar_class = nil) #:nodoc:
      @raw_driver = raw_drv
      @raw_connection = raw_conn
      @activerecord_class = ar_class
    end
    
    def self.create(raw_conn, ar_class = nil) #:nodoc:
      if ar_class && !(defined?(::ActiveRecord) && [ar_class, ar_class.superclass].include?(::ActiveRecord::Base))
        raise ArgumentError, "Wrong ActiveRecord class"
      end
      # MRI 1.8.6 or YARV 1.9.1
      if (!defined?(RUBY_ENGINE) || RUBY_ENGINE == "ruby") && defined?(OCI8)
        OCIConnection.new(:oci, raw_conn, ar_class)
      # JRuby
      elsif (defined?(RUBY_ENGINE) && RUBY_ENGINE == "jruby")
        JDBCConnection.new(:jdbc, raw_conn, ar_class)
      else
        raise ArgumentError, "Unknown raw driver"
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
      raise NoMethodError, "Not implemented for this raw driver"
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

    def select_first(sql, *bindvars) #:nodoc:
      cursor = cursor_from_query(self, sql, *bindvars)
      cursor.fetch
    ensure
      cursor.close rescue nil
    end

    def select_hash_first(sql, *bindvars) #:nodoc:
      cursor = cursor_from_query(self, sql, *bindvars)
      cursor.fetch_hash
    ensure
      cursor.close rescue nil
    end

    def select_all(sql, *bindvars, &block) #:nodoc:
      cursor = cursor_from_query(self, sql, *bindvars)
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
      cursor = cursor_from_query(self, sql, *bindvars)
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

    def arrays_to_hash(keys, values) #:nodoc:
      (0...keys.size).inject({}) { |hash, i| hash[keys[i]] = values[i]; hash }
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
        (row = fetch) && @connection.arrays_to_hash(fields, row)
      end
    end

  end

end