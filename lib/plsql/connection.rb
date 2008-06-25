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
  
  class OCIConnection < Connection
    
    def logoff
      raw_connection.logoff
    end
    
    def select_first(sql, *bindvars)
      begin
        cursor = raw_connection.exec(sql, *bindvars)
        result = cursor.fetch
        if result
          result.map { |val| ora_value_to_ruby_value(val) }
        else
          nil
        end
      ensure
        cursor.close rescue nil
      end
    end

    def select_all(sql, *bindvars, &block)
      begin
        cursor = raw_connection.exec(sql, *bindvars)
        results = []
        row_count = 0
        while row = cursor.fetch
          row_with_typecast = row.map {|val| ora_value_to_ruby_value(val) }
          if block_given?
            yield(row_with_typecast)
            row_count += 1
          else
            results << row_with_typecast
          end
        end
        block_given? ? row_count : results
      ensure
        cursor.close rescue nil
      end
    end

    def exec(sql, *bindvars)
      raw_connection.exec(sql, *bindvars)
    end

    class Cursor
      attr_accessor :raw_cursor
      
      def initialize(raw_cur)
        @raw_cursor = raw_cur
      end

      def bind_param(key, value, type=nil, length=nil)
        raw_cursor.bind_param(key, value, type, length)
      end
      
      def exec(*bindvars)
        raw_cursor.exec(*bindvars)
      end

      def [](key)
        raw_cursor[key]
      end

      def close
        raw_cursor.close
      end
    end

    def parse(sql)
      Cursor.new(raw_connection.parse(sql))
    end
    

    private
    
    def ora_number_to_ruby_number(num)
      num.to_i == num.to_f ? num.to_i : num.to_f
    end
    
    def ora_date_to_ruby_date(val)
      val.to_time
    end

    def ora_value_to_ruby_value(val)
      case val
      when Float, OraNumber
        ora_number_to_ruby_number(val)
      when OraDate
        ora_date_to_ruby_date(val)
      else
        val
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