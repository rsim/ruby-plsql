module PLSQL
  class Schema
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
    
    def connection=(raw_conn)
      @connection = raw_conn ? Connection.create(raw_conn) : nil
      if @connection
        @procedures = {}
        @packages = {}
        @schemas = {}
      else
        @procedures = nil
        @packages = nil
        @schemas = nil
      end
    end
    
    def logoff
      connection.logoff
      self.connection = nil
    end

    def schema_name
      return nil unless connection
      @schema_name ||= select_first("SELECT SYS_CONTEXT('userenv','session_user') FROM dual")[0]
    end

    def select_first(sql, *bindvars)
      # cursor = connection.exec(sql, *bindvars)
      # result = cursor.fetch
      # cursor.close
      # result
      connection.select_first(sql, *bindvars)
    end
    
    def commit
      connection.commit
    end

    def rollback
      connection.rollback
    end
    
    # Set to :local or :utc
    @@default_timezone = :local
    def default_timezone
      @@default_timezone
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
    
    def method_missing(method, *args)
      raise ArgumentError, "No PL/SQL connection" unless connection
      if procedure = @procedures[method]
        procedure.exec(*args)
      elsif procedure = Procedure.find(self, method)
        @procedures[method] = procedure
        procedure.exec(*args)
      elsif package = @packages[method]
        package
      elsif package = Package.find(self, method)
        @packages[method] = package
      elsif schema = @schemas[method]
        schema
      elsif schema = find_other_schema(method)
        @schemas[method] = schema
      else
        raise ArgumentError, "No PL/SQL procedure found"
      end
    end

    def find_other_schema(name)
      return nil unless @first && connection
      if select_first("SELECT username FROM all_users WHERE username = :username", name.to_s.upcase)
        Schema.new(connection.raw_connection, name, false)
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
