begin
  require "java"
  require "jruby"

  # ojdbc14.jar file should be in JRUBY_HOME/lib or should be in ENV['PATH'] or load path

  ojdbc_jar = "ojdbc14.jar"

  unless ENV_JAVA['java.class.path'] =~ Regexp.new(ojdbc_jar)
    # On Unix environment variable should be PATH, on Windows it is sometimes Path
    env_path = ENV["PATH"] || ENV["Path"] || ''
    if ojdbc_jar_path = env_path.split(/[:;]/).concat($LOAD_PATH).find{|d| File.exists?(File.join(d,ojdbc_jar))}
      require File.join(ojdbc_jar_path,ojdbc_jar)
    end
  end

  java.sql.DriverManager.registerDriver Java::oracle.jdbc.driver.OracleDriver.new

  # set tns_admin property from TNS_ADMIN environment variable
  if !java.lang.System.get_property("oracle.net.tns_admin") && ENV["TNS_ADMIN"]
    java.lang.System.set_property("oracle.net.tns_admin", ENV["TNS_ADMIN"])
  end

rescue LoadError, NameError
  # JDBC driver is unavailable.
  error_message = "ERROR: ruby-plsql could not load Oracle JDBC driver. "+
                  "Please install ojdbc14.jar library."
  STDERR.puts error_message
  raise LoadError
end

module PLSQL
  class JDBCConnection < Connection
    def logoff
      raw_connection.close
      true
    rescue
      false
    end

    def commit
      raw_connection.commit
    end

    def rollback
      raw_connection.rollback
    end

    def autocommit?
      raw_connection.getAutoCommit
    end

    def autocommit=(value)
      raw_connection.setAutoCommit(value)
    end

    def select_first(sql, *bindvars)
      stmt = prepare_statement(sql, *bindvars)
      rset = stmt.executeQuery
      metadata = rset.getMetaData
      column_count = metadata.getColumnCount
      if rset.next
        (1..column_count).map do |i|
          get_ruby_value_from_result_set(rset,i,metadata.getColumnTypeName(i))
        end
      else
        nil
      end
    ensure
      rset.close rescue nil
      stmt.close rescue nil
    end

    def select_all(sql, *bindvars, &block)
      stmt = prepare_statement(sql, *bindvars)
      results = []
      row_count = 0
      rset = stmt.executeQuery
      metadata = rset.getMetaData
      column_count = metadata.getColumnCount
      while rset.next
        row_with_typecast = (1..column_count).map do |i|
          get_ruby_value_from_result_set(rset,i,metadata.getColumnTypeName(i))
        end
        if block_given?
          yield(row_with_typecast)
          row_count += 1
        else
          results << row_with_typecast
        end
      end
      block_given? ? row_count : results
    ensure
      rset.close rescue nil
      stmt.close rescue nil
    end
    
    def exec(sql, *bindvars)
      cs = prepare_call(sql, *bindvars)
      cs.execute
      true
    ensure
      cs.close rescue nil
    end

    class Cursor

      def initialize(sql, conn)
        @sql = sql
        @connection = conn
        @params = sql.scan(/\:\w+/)
        @out_types = {}
        @out_index = {}
        @statement = @connection.prepare_call(sql)
      end

      def bind_param(key, value, type=nil, length=nil, in_out='IN')
        @connection.set_bind_variable(@statement, key, value, type, length)
        if in_out =~ /OUT/
          @out_types[key] = type || value.class
          @out_index[key] = bind_param_index(key)
          @statement.registerOutParameter(@out_index[key],@connection.get_java_sql_type(value,type))
        end
      end
      
      def exec
        @statement.execute
      end

      def [](key)
        @connection.get_bind_variable(@statement, @out_index[key], @out_types[key])
      end

      def close
        @statement.close
      end
      
      private
      
      def bind_param_index(key)
        return key if key.kind_of? Integer
        key = ":#{key.to_s}" unless key.to_s =~ /^:/
        @params.index(key)+1
      end
    end

    def parse(sql)
      Cursor.new(sql, self)
    end

    def prepare_statement(sql, *bindvars)
      stmt = raw_connection.prepareStatement(sql)
      bindvars.each_with_index do |bv, i|
        set_bind_variable(stmt, i+1, bv)
      end
      stmt
    end

    def prepare_call(sql, *bindvars)
      stmt = raw_connection.prepareCall(sql)
      bindvars.each_with_index do |bv, i|
        set_bind_variable(stmt, i+1, bv)
      end
      stmt
    end

    def get_java_sql_type(value, type)
      case type ? type.to_s : value.class.to_s
      when 'Fixnum', 'Bignum', 'Integer'
        java.sql.Types::INTEGER
      when 'Float'
        java.sql.Types::FLOAT
      when 'BigDecimal'
        java.sql.Types::NUMERIC
      when 'String'
        java.sql.Types::VARCHAR
      when 'Java::OracleSql::CLOB'
        Java::oracle.jdbc.OracleTypes::CLOB
      when 'Java::OracleSql::BLOB'
        Java::oracle.jdbc.OracleTypes::BLOB
      when 'Date'
        java.sql.Types::DATE
      when 'Time'
        java.sql.Types::DATE
      when 'DateTime'
        java.sql.Types::DATE
      else
        java.sql.Types::VARCHAR
      end
    end

    def set_bind_variable(stmt, i, value, type=nil, length=nil)
      key = i.kind_of?(Integer) ? nil : i.to_s.gsub(':','')
      case !value.nil? && type ? type.to_s : value.class.to_s
      when 'Fixnum', 'Bignum', 'Integer'
        stmt.send("setInt#{key && "AtName"}", key || i, value)
      when 'Float'
        stmt.send("setFloat#{key && "AtName"}", key || i, value)
      when 'BigDecimal'
        stmt.send("setBigDecimal#{key && "AtName"}", key || i, java.math.BigDecimal.new(value.to_s))
      when 'String'
        stmt.send("setString#{key && "AtName"}", key || i, value)
      when 'Java::OracleSql::CLOB'
        stmt.send("setClob#{key && "AtName"}", key || i, value)
      when 'Java::OracleSql::BLOB'
        stmt.send("setBlob#{key && "AtName"}", key || i, value)
      when 'Date', 'Time', 'DateTime'
        stmt.send("setDATE#{key && "AtName"}", key || i, Java::oracle.sql.DATE.new(value.strftime("%Y-%m-%d %H:%M:%S")))
      when 'NilClass'
        stmt.send("setNull#{key && "AtName"}", key || i, get_java_sql_type(value, type))
      end
    end
    
    def get_bind_variable(stmt, i, type)
      case type.to_s
      when 'Fixnum', 'Bignum', 'Integer'
        stmt.getInt(i)
      when 'Float'
        stmt.getFloat(i)
      when 'BigDecimal'
        bd = stmt.getBigDecimal(i)
        bd && BigDecimal.new(bd.to_s)
      when 'String'
        stmt.getString(i)
      when 'Java::OracleSql::CLOB'
        stmt.getClob(i)
      when 'Java::OracleSql::BLOB'
        stmt.getBlob(i)
      when 'Date','Time','DateTime'
        if dt = stmt.getDATE(i)
          d = dt.dateValue
          t = dt.timeValue
          Time.send(plsql.default_timezone, d.year + 1900, d.month + 1, d.date, t.hours, t.minutes, t.seconds)
        else
          nil
        end
      end
    end

    def get_ruby_value_from_result_set(rset, i, type_name)
      case type_name
      when "CHAR", "VARCHAR2"
        rset.getString(i)
      when "CLOB"
        ora_value_to_ruby_value(rset.getClob(i))
      when "BLOB"
        ora_value_to_ruby_value(rset.getBlob(i))
      when "NUMBER"
        d = rset.getBigDecimal(i)
        if d.nil?
          nil
        elsif d.scale == 0
          d.toBigInteger+0
        else
          BigDecimal(d.toString)
        end
      when "DATE"
        if dt = rset.getDATE(i)
          d = dt.dateValue
          t = dt.timeValue
          Time.send(plsql.default_timezone, d.year + 1900, d.month + 1, d.date, t.hours, t.minutes, t.seconds)
        else
          nil
        end
      when /^TIMESTAMP/
        ts = rset.getTimestamp(i)
        ts && Time.send(Base.default_timezone, ts.year + 1900, ts.month + 1, ts.date, ts.hours, ts.minutes, ts.seconds,
          ts.nanos / 1000)
      else
        nil
      end
    end
    
    def plsql_to_ruby_data_type(data_type, data_length)
      case data_type
      when "VARCHAR2"
        [String, data_length || 32767]
      when "CLOB"
        [Java::OracleSql::CLOB, nil]
      when "BLOB"
        [Java::OracleSql::BLOB, nil]
      when "NUMBER"
        [BigDecimal, nil]
      when "DATE"
        [Time, nil]
      when "TIMESTAMP"
        [Time, nil]
      else
        [String, 32767]
      end
    end

    def ruby_value_to_ora_value(val, type)
      if type == BigDecimal
        val.nil? || val.is_a?(Fixnum) || val.is_a?(BigDecimal) ? val : BigDecimal(val.to_s)
      elsif type == Time
        case val
        when DateTime
          Time.send(plsql.default_timezone, val.year, val.month, val.day, val.hour, val.min, val.sec)
        when Date
          Time.send(plsql.default_timezone, val.year, val.month, val.day, 0, 0, 0)
        else
          val
        end
      elsif type == Java::OracleSql::CLOB
        if val
          clob = Java::OracleSql::CLOB.createTemporary(raw_connection, false, Java::OracleSql::CLOB::DURATION_SESSION)
          clob.setString(1,val)
          clob
        else
          Java::OracleSql::CLOB.getEmptyCLOB
        end
      elsif type == Java::OracleSql::BLOB
        if val
          blob = Java::OracleSql::BLOB.createTemporary(raw_connection, false, Java::OracleSql::BLOB::DURATION_SESSION)
          blob.setBytes(1, val.to_java_bytes)
          blob
        else
          Java::OracleSql::BLOB.getEmptyBLOB
        end
      else
        val
      end
    end

    def ora_value_to_ruby_value(val)
      case val
      when Float, BigDecimal
        ora_number_to_ruby_number(val)
      when Java::OracleSql::CLOB
        if val.isEmptyLob
          nil
        else
          val.getSubString(1, val.length)
        end
      when Java::OracleSql::BLOB
        if val.isEmptyLob
          nil
        else
          String.from_java_bytes(val.getBytes(1, val.length))
        end
      else
        val
      end
    end

    private
    
    def ora_number_to_ruby_number(num)
      # return BigDecimal instead of Float to avoid rounding errors
      # num.to_i == num.to_f ? num.to_i : num.to_f
      num == (num_to_i = num.to_i) ? num_to_i : BigDecimal.new(num.to_s)
    end
    
  end
  
end