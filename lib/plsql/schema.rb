module PLSQL
  class Schema
    include SQLStatements

    @@schemas = {}
    
    class <<self
      def find_or_new(connection_alias)
        connection_alias ||= :default
        if @@schemas[connection_alias]
          @@schemas[connection_alias]
        else
          @@schemas[connection_alias] = self.new
        end
      end

    end
    
    def initialize(raw_conn = nil, schema = nil, first = true)
      self.connection = raw_conn
      @schema_name = schema ? schema.to_s.upcase : nil
      @first = first
    end
    
    def connection
      @connection
    end
    
    def raw_connection=(raw_conn)
      @connection = raw_conn ? Connection.create(raw_conn) : nil
      reset_instance_variables
    end

    def connection=(conn)
      if conn.is_a?(::PLSQL::Connection)
        @connection = conn
        reset_instance_variables
      else
        self.raw_connection = conn
      end
    end

    def activerecord_class=(ar_class)
      @connection = ar_class ? Connection.create(nil, ar_class) : nil
      reset_instance_variables
    end
    
    def logoff
      @connection.logoff
      self.connection = nil
    end

    def schema_name
      return nil unless connection
      @schema_name ||= select_first("SELECT SYS_CONTEXT('userenv','session_user') FROM dual")[0]
    end

    # Set to :local or :utc
    @@default_timezone = nil
    def default_timezone
      @@default_timezone ||
        # Use ActiveRecord class default_timezone when ActiveRecord connection is used
        (@connection && (ar_class = @connection.activerecord_class) && ar_class.default_timezone) ||
        # default to local timezone
        :local
    end
    
    def default_timezone=(value)
      if [:local, :utc].include?(value)
        @@default_timezone = value
      else
        raise ArgumentError, "default timezone should be :local or :utc"
      end
    end

    # Same implementation as for ActiveRecord
    # DateTimes aren't aware of DST rules, so use a consistent non-DST offset when creating a DateTime with an offset in the local zone
    def local_timezone_offset
      ::Time.local(2007).utc_offset.to_r / 86400
    end
    
    private

    def reset_instance_variables
      if @connection
        @schema_objects = {}
      else
        @schema_objects = nil
      end
      @schema_name = nil
      @@default_timezone = nil
    end
    
    def method_missing(method, *args, &block)
      raise ArgumentError, "No PL/SQL connection" unless connection
      # look in cache at first
      if schema_object = @schema_objects[method]
        if schema_object.is_a?(Procedure)
          schema_object.exec(*args, &block)
        else
          schema_object
        end
      # search in database
      elsif procedure = Procedure.find(self, method)
        @schema_objects[method] = procedure
        procedure.exec(*args, &block)
      elsif package = Package.find(self, method)
        @schema_objects[method] = package
      elsif table = Table.find(self, method)
        @schema_objects[method] = table
      elsif sequence = Sequence.find(self, method)
        @schema_objects[method] = sequence
      elsif schema = find_other_schema(method)
        @schema_objects[method] = schema
      else
        raise ArgumentError, "No PL/SQL procedure found"
      end
    end

    def find_other_schema(name)
      return nil unless @first && connection
      if select_first("SELECT username FROM all_users WHERE username = :username", name.to_s.upcase)
        Schema.new(connection, name, false)
      else
        nil
      end
    end
    
  end
end

module Kernel
  def plsql(connection_alias = nil)
    PLSQL::Schema.find_or_new(connection_alias)
  end
end
