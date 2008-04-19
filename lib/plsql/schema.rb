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
    
    def initialize(conn = nil, schema = nil, first = true)
      self.connection = conn
      @schema_name = schema ? schema.to_s.upcase : nil
      @first = first
    end
    
    def connection
      @connection
    end
    
    def connection=(conn)
      @connection = conn
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
      cursor = connection.exec(sql, *bindvars)
      result = cursor.fetch
      cursor.close
      result
    end
    
    def commit
      connection.commit
    end

    def rollback
      connection.rollback
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
