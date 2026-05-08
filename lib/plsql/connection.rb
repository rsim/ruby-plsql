module PLSQL
  class Connection
    attr_reader :raw_driver
    attr_reader :activerecord_class

    # `:database` is the primary option and is treated as a service name.
    # `:service_name` is provided for compatibility with the
    # oracle-enhanced adapter (rsim/oracle-enhanced#2669) and is treated
    # as an alias of `:database`. `:sid` selects the legacy SID URL form
    # for single-instance Oracle deployments (e.g. 11g XE), and exists to
    # replace the deprecated `database: ":SID"` colon-prefix entry. The
    # three options are mutually exclusive.
    #
    # SID character set matches the oracle-enhanced adapter: alphanumeric,
    # underscore, `$`, `#`. No length cap — INSTANCE_NAME allows up to 255
    # characters in 19c+; the historical 8-char limit applies to DB_NAME,
    # not to the SID/INSTANCE_NAME the listener registers under.
    SID_IDENTIFIER_PATTERN = /\A[\w$#]+\z/

    # Validates :database / :service_name / :sid in `params` and folds
    # :service_name into :database so downstream URL builders only need to
    # branch on :database vs :sid. Raises ArgumentError on conflicts or
    # invalid values.
    def self.resolve_database_aliases!(params)
      provided_keys = []
      provided_keys << ":database"     if params[:database]
      provided_keys << ":service_name" if params[:service_name]
      provided_keys << ":sid"          if params[:sid]
      if provided_keys.size > 1
        raise ArgumentError,
          "Cannot specify more than one of #{provided_keys.join(', ')}; they are mutually exclusive."
      end

      if (svc = params[:service_name])
        if svc.to_s.start_with?("/")
          raise ArgumentError,
            "Invalid :service_name value #{svc.inspect}; must not start with '/'."
        end
        params[:database] = svc
      end

      if (sid = params[:sid]) && !sid.to_s.match?(SID_IDENTIFIER_PATTERN)
        raise ArgumentError,
          "Invalid :sid value #{sid.inspect}; must be an Oracle SID (alphanumeric, underscore, $, #)."
      end
    end

    def initialize(raw_conn, ar_class = nil) # :nodoc:
      @raw_driver = self.class.driver_type
      @raw_connection = raw_conn
      @activerecord_class = ar_class
    end

    def self.create(raw_conn, ar_class = nil) # :nodoc:
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

    def self.create_new(params) # :nodoc:
      conn = case driver_type
             when :oci
               OCIConnection.create_raw(params)
             when :jdbc
               JDBCConnection.create_raw(params)
             else
               raise ArgumentError, "Unknown raw driver"
      end
      conn.set_time_zone(params[:time_zone] || ENV["ORA_SDTZ"])
      conn
    end

    def self.driver_type # :nodoc:
      # MRI 1.8.6 or YARV 1.9.1 or TruffleRuby
      @driver_type ||= if (!defined?(RUBY_ENGINE) || RUBY_ENGINE == "ruby" || RUBY_ENGINE == "truffleruby") && defined?(OCI8)
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

    def logoff # :nodoc:
      # Rollback any uncommited transactions
      rollback
      # Common cleanup activities before logoff, should be called from particular driver method
      drop_session_ruby_temporary_tables
    end

    def commit # :nodoc:
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def rollback # :nodoc:
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

    def select_first(sql, *bindvars) # :nodoc:
      cursor = cursor_from_query(sql, bindvars, prefetch_rows: 1)
      cursor.fetch
    ensure
      cursor.close rescue nil
    end

    def select_hash_first(sql, *bindvars) # :nodoc:
      cursor = cursor_from_query(sql, bindvars, prefetch_rows: 1)
      cursor.fetch_hash
    ensure
      cursor.close rescue nil
    end

    def select_all(sql, *bindvars, &block) # :nodoc:
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

    def select_hash_all(sql, *bindvars, &block) # :nodoc:
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

    def exec(sql, *bindvars) # :nodoc:
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def parse(sql) # :nodoc:
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

    def describe_synonym(schema_name, synonym_name) # :nodoc:
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
    def set_time_zone(time_zone = nil)
      exec("alter session set time_zone = '#{time_zone}'") if time_zone
    end

    # Returns session time zone
    def time_zone
      select_first("SELECT SESSIONTIMEZONE FROM dual")[0]
    end

    RUBY_TEMP_TABLE_PREFIX = "ruby_"

    # Drop all ruby temporary tables that are used for calling packages with table parameter types defined in packages
    def drop_all_ruby_temporary_tables
      select_all("SELECT table_name FROM user_tables WHERE temporary='Y' AND table_name LIKE :table_name",
                  RUBY_TEMP_TABLE_PREFIX.upcase + "%").each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end

    # Drop ruby temporary tables created in current session that are used for calling packages with table parameter types defined in packages
    def drop_session_ruby_temporary_tables
      select_all("SELECT table_name FROM user_tables WHERE temporary='Y' AND table_name LIKE :table_name",
                  RUBY_TEMP_TABLE_PREFIX.upcase + "#{session_id}_%").each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end
  end
end
