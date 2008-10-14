module PLSQL
  class Connection
    attr_reader :raw_connection
    attr_reader :raw_driver

    def initialize(raw_drv, raw_conn)
      @raw_driver = raw_drv
      @raw_connection = raw_conn
    end
    
    def self.create(raw_conn)
      if !raw_conn.respond_to?(:java_class) && defined?(OCI8)
        OCIConnection.new(:oci, raw_conn)
      elsif raw_conn.respond_to?(:java_class) && raw_conn.java_class.to_s =~ /jdbc/
        JDBCConnection.new(:jdbc, raw_conn)
      else
        raise ArgumentError, "Unknown raw driver"
      end
    end
    
    def oci?
      @raw_driver == :oci
    end

    def jdbc?
      @raw_driver == :jdbc
    end
    
    def logoff
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def commit
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def rollback
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def autocommit?
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def autocommit=(value)
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def select_first(sql, *bindvars)
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def select_all(sql, *bindvars, &block)
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def exec(sql, *bindvars)
      raise NoMethodError, "Not implemented for this raw driver"
    end

    def parse(sql)
      raise NoMethodError, "Not implemented for this raw driver"
    end

  end

end