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
    
    def select_first(sql, *bindvars)
      raise NoMethodError, "Not implemented for this raw driver"
    end

    # def exec(sql, *bindvars)
    #   @raw_connection.exec(sql, *bindvars)
    # end

  end
  
  class OCIConnection < Connection
    def select_first(sql, *bindvars)
      begin
        cursor = raw_connection.exec(sql, *bindvars)
        result = cursor.fetch
        if result
          result.map do |val|
            case val
            when Float
              val == val.to_i ? val.to_i : val
            when OraDate
              val.to_time
            else
              val
            end
          end
        else
          nil
        end
      ensure
        cursor.close rescue nil
      end
    end

  end
  
  class JDBCConnection < Connection
    def select_first(sql, *bindvars)
      sql = sql.gsub(/:\w+/,'?')
      stmt = raw_connection.prepareStatement(sql)
      bindvars.each_with_index do |bv, i|
        case bv
        when Integer
          stmt.setInt(i+1, bv)
        when String
          stmt.setString(i+1, bv)
        end
      end
      rset = stmt.executeQuery
      if rset.next
        metadata = rset.getMetaData
        (1..(metadata.getColumnCount)).map do |i|
          case metadata.getColumnTypeName(i)
          when "CHAR", "VARCHAR2"
            rset.getString(i)
          when "NUMBER"
            d = rset.getBigDecimal(i)
            if d.scale == 0
              d.longValue 
            else
              d.doubleValue
            end
          when "DATE"
            # FIXME 
            ts = rset.getTimestamp(i)
            Time.local(ts.year, ts.month, ts.day, ts.hours, ts.minutes, ts.seconds, 0)
          else
            nil
          end
        end
      else
        nil
      end
    end

  end

end