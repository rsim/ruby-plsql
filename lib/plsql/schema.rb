require "plsql/schema_helpers"

module PLSQL
  class Schema
    
    include SQLStatements
    include SchemaHelperProvider

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
    end

    # Returns connection wrapper object (this is not raw OCI8 or JDBC connection!)
    attr_reader :connection
    
    def schema_name
      # Placeholder to be replaced by database-specific method.
    end

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
    #     "jdbc:oracle:thin:@#{database_host}:#{database_port}:#{database_name}",
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
      @connection = ar_class ? Connection.create(nil, :ar_class => ar_class) : nil
      reset_instance_variables
      ar_class
    end

    # Disconnect from Oracle
    def logoff
      @connection.logoff
      self.connection = nil
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
      raise NoMethodError, "Not implemented for this raw driver"
    end

    # IO stream where to log DBMS_OUTPUT from PL/SQL procedures.
    def dbms_output_stream
      raise NoMethodError, "Not implemented for this raw driver"
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
      
      # Whenever the connection is changed,
      # extend with correct database-specific schema-helper.
      extend schema_helper(connection.dialect) if connection
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

    def find_standard_procedure(name)
      return nil if @original_schema
      Procedure.find(self, name, 'STANDARD', 'SYS')
    end

    def find_public_synonym(name)
      return nil if @original_schema
      target_schema_name, target_object_name = @connection.describe_synonym('PUBLIC', name)
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
