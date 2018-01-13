module PLSQL
  class Schema
    include SQLStatements

    @@schemas = {}

    class <<self
      def find_or_new(connection_alias) #:nodoc:
        connection_alias ||= :default
        if @@schemas[connection_alias]
          @@schemas[connection_alias]
        else
          @@schemas[connection_alias] = self.new
        end
      end
    end

    def initialize(raw_conn = nil, schema = nil, original_schema = nil) #:nodoc:
      self.connection = raw_conn
      @schema_name = schema ? schema.to_s.upcase : nil
      @original_schema = original_schema
      @dbms_output_stream = nil
    end

    # Returns connection wrapper object (this is not raw OCI8 or JDBC connection!)
    attr_reader :connection

    def root_schema #:nodoc:
      @original_schema || self
    end

    def raw_connection=(raw_conn) #:nodoc:
      @connection = raw_conn ? Connection.create(raw_conn) : nil
      reset_instance_variables
    end

    # Set connection to OCI8 or JDBC connection:
    #
    #   plsql.connection = OCI8.new(database_user, database_password, database_name)
    #
    # or
    #
    #   plsql.connection = java.sql.DriverManager.getConnection(
    #     "jdbc:oracle:thin:@#{database_host}:#{database_port}/#{database_service_name}",
    #     database_user, database_password)
    #
    def connection=(conn)
      if conn.is_a?(Connection)
        @connection = conn
        reset_instance_variables
      else
        self.raw_connection = conn
      end
      conn
    end

    # Create new OCI8 or JDBC connection using one of the following ways:
    #
    #   plsql.connect! username, password, database_tns_alias
    #   plsql.connect! username, password, :host => host, :port => port, :database => database
    #   plsql.connect! :username => username, :password => password, :database => database_tns_alias
    #   plsql.connect! :username => username, :password => password, :host => host, :port => port, :database => database
    #
    def connect!(*args)
      params = {}
      params[:username] = args.shift if args[0].is_a?(String)
      params[:password] = args.shift if args[0].is_a?(String)
      params[:database] = args.shift if args[0].is_a?(String)
      params.merge!(args.shift) if args[0].is_a?(Hash)
      raise ArgumentError, "Wrong number of arguments" unless args.empty?
      self.connection = Connection.create_new(params)
    end

    # Set connection to current ActiveRecord connection (use in initializer file):
    #
    #   plsql.activerecord_class = ActiveRecord::Base
    #
    def activerecord_class=(ar_class)
      @connection = ar_class ? Connection.create(nil, ar_class) : nil
      reset_instance_variables
      ar_class
    end

    # Disconnect from Oracle
    def logoff
      @connection.logoff
      self.connection = nil
    end

    # Current Oracle schema name
    def schema_name
      return nil unless connection
      @schema_name ||= select_first("SELECT SYS_CONTEXT('userenv','current_schema') FROM dual")[0]
    end

    # Default timezone to which database values will be converted - :utc or :local
    def default_timezone
      if @original_schema
        @original_schema.default_timezone
      else
        @default_timezone ||
          # Use ActiveRecord class default_timezone when ActiveRecord connection is used
          (@connection && (ar_class = @connection.activerecord_class) && ar_class.default_timezone) ||
          # default to local timezone
          :local
      end
    end

    # Set default timezone to which database values will be converted - :utc or :local
    def default_timezone=(value)
      if [:local, :utc].include?(value)
        @default_timezone = value
      else
        raise ArgumentError, "default timezone should be :local or :utc"
      end
    end

    # Same implementation as for ActiveRecord
    # DateTimes aren't aware of DST rules, so use a consistent non-DST offset when creating a DateTime with an offset in the local zone
    def local_timezone_offset #:nodoc:
      ::Time.local(2007).utc_offset.to_r / 86400
    end

    # DBMS_OUTPUT buffer size (default is 20_000)
    def dbms_output_buffer_size
      if @original_schema
        @original_schema.dbms_output_buffer_size
      else
        @dbms_output_buffer_size || 20_000
      end
    end

    # Seet DBMS_OUTPUT buffer size (default is 20_000). Example:
    #
    #   plsql.dbms_output_buffer_size = 100_000
    #
    def dbms_output_buffer_size=(value)
      @dbms_output_buffer_size = value
    end

    # Maximum line numbers for DBMS_OUTPUT in one PL/SQL call (from DBMSOUTPUT_LINESARRAY type)
    DBMS_OUTPUT_MAX_LINES = 2147483647

    # Specify IO stream where to log DBMS_OUTPUT from PL/SQL procedures. Example:
    #
    #   plsql.dbms_output_stream = STDOUT
    #
    def dbms_output_stream=(stream)
      @dbms_output_stream = stream
      if @dbms_output_stream.nil? && @connection
        sys.dbms_output.disable
      end
    end

    # IO stream where to log DBMS_OUTPUT from PL/SQL procedures.
    def dbms_output_stream
      if @original_schema
        @original_schema.dbms_output_stream
      else
        @dbms_output_stream
      end
    end

    private

      def reset_instance_variables
        if @connection
          @schema_objects = {}
        else
          @schema_objects = nil
        end
        @schema_name = nil
        @default_timezone = nil
      end

      def method_missing(method, *args, &block)
        raise ArgumentError, "No database connection" unless connection
        # search in database if not in cache at first
        object = (@schema_objects[method] ||= find_database_object(method) || find_other_schema(method) ||
           find_public_synonym(method) || find_standard_procedure(method))

        raise ArgumentError, "No database object '#{method.to_s.upcase}' found" unless object

        if object.is_a?(Procedure)
          object.exec(*args, &block)
        elsif object.is_a?(Type) && !args.empty?
          object.new(*args, &block)
        else
          object
        end
      end

      def find_database_object(name, override_schema_name = nil)
        object_schema_name = override_schema_name || schema_name
        object_name = name.to_s.upcase
        if row = select_first(
          "SELECT o.object_type, o.object_id
          FROM all_objects o
          WHERE owner = :owner AND object_name = :object_name
          AND object_type IN ('PROCEDURE','FUNCTION','PACKAGE','TABLE','VIEW','SEQUENCE','TYPE','SYNONYM')",
            object_schema_name, object_name)
          object_type, object_id = row
          case object_type
          when "PROCEDURE", "FUNCTION"
            if (connection.database_version <=> [11, 1, 0, 0]) >= 0
              if row = select_first(
                "SELECT p.object_id FROM all_procedures p
                 WHERE p.owner = :owner
                   AND p.object_name = :object_name
                   AND p.object_type = :object_type",
                   object_schema_name, object_name, object_type)
                object_id = row[0]
              else
                raise ArgumentError, "Database object '#{object_schema_name}.#{object_name}' is not in valid status\n#{
                  _errors(object_schema_name, object_name, object_type)}"
              end
            end
            Procedure.new(self, name, nil, override_schema_name, object_id)
          when "PACKAGE"
            Package.new(self, name, override_schema_name)
          when "TABLE"
            Table.new(self, name, override_schema_name)
          when "VIEW"
            View.new(self, name, override_schema_name)
          when "SEQUENCE"
            Sequence.new(self, name, override_schema_name)
          when "TYPE"
            Type.new(self, name, override_schema_name)
          when "SYNONYM"
            target_schema_name, target_object_name = @connection.describe_synonym(object_schema_name, object_name)
            find_database_object(target_object_name, target_schema_name)
          end
        end
      end

      def _errors(object_schema_name, object_name, object_type)
        result = ""
        previous_line = 0
        select_all(
          "SELECT e.line, e.position, e.text error_text, s.text source_text
          FROM all_errors e, all_source s
          WHERE e.owner = :owner AND e.name = :name AND e.type = :type
            AND s.owner = e.owner AND s.name = e.name AND s.type = e.type AND s.line = e.line
          ORDER BY e.sequence",
          object_schema_name, object_name, object_type
        ).each do |line, position, error_text, source_text|
          result << "Error on line #{'%4d' % line}: #{source_text}" if line > previous_line
          result << "     position #{'%4d' % position}: #{error_text}\n"
          previous_line = line
        end
        result unless result.empty?
      end

      def find_other_schema(name)
        return nil if @original_schema
        if select_first("SELECT username FROM all_users WHERE username = :username", name.to_s.upcase)
          Schema.new(connection, name, self)
        else
          nil
        end
      end

      def find_standard_procedure(name)
        return nil if @original_schema
        Procedure.find(self, name, "STANDARD", "SYS")
      end

      def find_public_synonym(name)
        return nil if @original_schema
        target_schema_name, target_object_name = @connection.describe_synonym("PUBLIC", name)
        find_database_object(target_object_name, target_schema_name) if target_schema_name
      end
  end
end

module Kernel
  # Returns current schema object. You can now chain either database object (packages, procedures, tables, sequences)
  # in current schema or specify different schema name. Examples:
  #
  #   plsql.test_function('some parameter')
  #   plsql.test_package.test_function('some parameter')
  #   plsql.other_schema.test_package.test_function('some parameter')
  #   plsql.table_name.all
  #   plsql.other_schema.table_name.all
  #
  def plsql(connection_alias = nil)
    PLSQL::Schema.find_or_new(connection_alias)
  end
end
