begin
  require "java"
  require "jruby"

  # postgresql-9.0-801.jdbc3.jar file should be in JRUBY_HOME/lib or should be in ENV['PATH'] or load path

  ojdbc_jar = "postgresql-9.0-801.jdbc3.jar"

  unless ENV_JAVA['java.class.path'] =~ Regexp.new(ojdbc_jar)
    # On Unix environment variable should be PATH, on Windows it is sometimes Path
    env_path = ENV["PATH"] || ENV["Path"] || ''
    if ojdbc_jar_path = env_path.split(/[:;]/).concat($LOAD_PATH).find{|d| File.exists?(File.join(d,ojdbc_jar))}
      require File.join(ojdbc_jar_path, ojdbc_jar)
    end
  end

  java.sql.DriverManager.registerDriver Java::org.postgresql.Driver.new

rescue LoadError, NameError
  # JDBC driver is unavailable.
  error_message = "ERROR: ruby-plsql could not load Postgres JDBC driver. "+
    "Please install #{ojdbc_jar} library."
  STDERR.puts error_message
  raise LoadError
end

require "plsql/connection_helpers"

module PLSQL
  class JDBCPGConnection < Connection #:nodoc:
    
    include PGConnectionHelper
    include JDBCConnectionHelper
    
    def self.create_raw(params)
      url = params[:url] || "jdbc:postgresql://#{params[:host] || 'localhost'}:#{params[:port] || 5432}/#{params[:database]}"
      new(java.sql.DriverManager.getConnection(url, params[:username], params[:password]), params)
    end
    
    def commit
      #raw_connection.commit
    end

    def rollback
      #raw_connection.rollback
    end
    
    def autocommit?
      raw_connection.getAutoCommit
    end

    def autocommit=(value)
      raw_connection.setAutoCommit(value)
    end
    
    def prefetch_rows=(value)
      #raw_connection.setDefaultRowPrefetch(value)
    end
    
    def create_callable_stmt(conn, sql, params = {})
      CallableStatement.new(conn, sql, params)
    end
    
    class CallableStatement #:nodoc:

      def initialize(conn, sql, params = {})
        @sql = sql
        @connection = conn
        @params = params
        @out_types = {}
        @out_index = {}
        @statement = @connection.prepare_call(sql)
      end

      def bind_param(arg, value, metadata)
        type, length = @connection.plsql_to_ruby_data_type(metadata)
        db_value = @connection.ruby_value_to_db_value(value, type, metadata)
        if metadata[:in_out] =~ /IN/
          @connection.set_bind_variable(@statement, bind_param_index(arg), db_value, type, length, metadata)
        end
        if metadata[:in_out] =~ /OUT/
          @out_types[arg] = type || db_value.class
          @out_index[arg] = bind_param_index(arg)
          @statement.registerOutParameter(@out_index[arg], @connection.get_java_sql_type(db_value, type))
        end
      end
      
      def exec
        @statement.execute
      end

      def [](key)
        @connection.db_value_to_ruby_value(@connection.get_bind_variable(@statement, @out_index[key], @out_types[key]))
      end

      def close
        @statement.close
      end
      
      private
      
      def bind_param_index(key)
        return key + 1 if key.kind_of? Integer
        @params[key] + 1
      end
    end
    
    RUBY_CLASS_TO_SQL_TYPE = {
      Fixnum            => java.sql.Types::INTEGER,
      Bignum            => java.sql.Types::BIGINT,
      Integer           => java.sql.Types::INTEGER,
      Float             => java.sql.Types::FLOAT,
      BigDecimal        => java.sql.Types::NUMERIC,
      String            => java.sql.Types::VARCHAR,
      java.lang.Boolean => java.sql.Types::BOOLEAN,
      java.sql.Clob     => java.sql.Types::CLOB,
      java.sql.Blob     => java.sql.Types::BLOB,
      Date              => java.sql.Types::DATE,
      Time              => java.sql.Types::TIMESTAMP,
      DateTime          => java.sql.Types::TIMESTAMP,
      java.sql.Array    => java.sql.Types::ARRAY,
      Array             => java.sql.Types::ARRAY,
      java.sql.Struct   => java.sql.Types::STRUCT,
      Hash              => java.sql.Types::STRUCT
    }

    SQL_TYPE_TO_RUBY_CLASS = {
      java.sql.Types::CHAR        => String,
      java.sql.Types::VARCHAR     => String,
      java.sql.Types::LONGVARCHAR => String,
      java.sql.Types::BOOLEAN     => java.lang.Boolean,
      java.sql.Types::BIT         => java.lang.Boolean,
      java.sql.Types::DOUBLE      => BigDecimal,
      java.sql.Types::NUMERIC     => BigDecimal,
      java.sql.Types::INTEGER     => Fixnum,
      java.sql.Types::BIGINT      => Bignum,
      java.sql.Types::DATE        => Date,
      java.sql.Types::TIME        => Time,
      java.sql.Types::TIMESTAMP   => DateTime,
      java.sql.Types::BLOB        => String,
      java.sql.Types::CLOB        => String,
      java.sql.Types::ARRAY       => java.sql.Array,
      java.sql.Types::STRUCT      => java.sql.Struct
    }
    
    def get_java_sql_type(value, type)
      RUBY_CLASS_TO_SQL_TYPE[type || value.class] || java.sql.Types::VARCHAR
    end
    
    def set_bind_variable(stmt, i, value, type=nil, length=nil, metadata={})
      key = i.kind_of?(Integer) ? nil : i.to_s
      type_symbol = (!value.nil? && type ? type : value.class).to_s.to_sym
      case type_symbol
      when :Fixnum, :Bignum, :Integer
        stmt.send("setInt#{key && "AtName"}", key || i, value)
      when :Float
        stmt.send("setFloat#{key && "AtName"}", key || i, value)
      when :BigDecimal, :'Java::JavaMath::BigDecimal'
        stmt.send("setBigDecimal#{key && "AtName"}", key || i, value)
      when :String
        stmt.send("setString#{key && "AtName"}", key || i, value)
      when :'Java::JavaLang::Boolean'
        stmt.send("setBoolean#{key && "AtName"}", key || i, value)
      when :'Java::JavaSql::Clob'
        stmt.send("setClob#{key && "AtName"}", key || i, value)
      when :'Java::JavaSql::Blob'
        stmt.send("setBlob#{key && "AtName"}", key || i, value)
      when :Date, :'Java::JavaSql::Date'
        stmt.send("setDate#{key && "AtName"}", key || i, value)
      when :DateTime, :Time, :'Java::JavaSql::Timestamp'
        stmt.send("setTimestamp#{key && "AtName"}", key || i, value)
      when :NilClass
        stmt.send("setNull#{key && "AtName"}", key || i, get_java_sql_type(value, type))
      when :'Java::JavaSql::Array'
        stmt.send("setArray#{key && "AtName"}", key || i, value)
      when :'Java::JavaSql::Struct'
        stmt.send("setObject#{key && "AtName"}", key || i, value)
      when :'Java::JavaSql::ResultSet'
        stmt.send("setCursor#{key && "AtName"}", key || i, value)
      else
        raise ArgumentError, "Don't know how to bind variable with type #{type_symbol}"
      end
    end
    
    def get_bind_variable(stmt, i, type)
      variable = case type.to_s.to_sym
      when :Fixnum, :Bignum, :Integer
        stmt.getInt(i)
      when :Float
        stmt.getFloat(i)
      when :BigDecimal
        bd = stmt.getBigDecimal(i)
        bd && BigDecimal.new(bd.to_s)
      when :String
        stmt.getString(i)
      when :'Java::JavaLang::Boolean'
        stmt.getBoolean(i)
      when :'Java::JavaSql::Clob'
        stmt.getClob(i)
      when :'Java::JavaSql::Blob'
        stmt.getBlob(i)
      when :Date
        stmt.getDate(i)
      when :DateTime, :Time
        stmt.getTimestamp(i)
      when :'Java::JavaSql::Array'
        stmt.getArray(i)
      when :'Java::JavaSql::Struct'
        stmt.getObject(i)
      when :'Java::JavaSql::ResultSet'
        stmt.getCursor(i)
      end
      variable unless stmt.wasNull()
    end
    
    def get_ruby_value_from_result_set(rset, i, metadata)
      ruby_type = SQL_TYPE_TO_RUBY_CLASS[metadata[:sql_type]]
      db_value = get_bind_variable(rset, i, ruby_type)
      db_value_to_ruby_value(db_value)
    end
    
    def plsql_to_ruby_data_type(metadata)
      data_type, data_length = metadata[:data_type], metadata[:data_length]
      case data_type
      when "VARCHAR", "CHAR"
        [String, data_length || 32767]
      when "TEXT"
        [String, nil]
      when "BOOLEAN"
        [Java::JavaLang::Boolean, nil]
      when "CLOB", "NCLOB"
        [Java::JavaSql::Clob, nil]
      when "BLOB"
        [Java::JavaSql::Bob, nil]
      when "NUMERIC"
        [BigDecimal, nil]
      when "INTEGER"
        [Fixnum, nil]
      when "DATE"
        [Date, nil]
      when "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE"
        [DateTime, nil]
      when "TIME", "TIME WITH TIME ZONE, TIME WITHOUT TIME ZONE"
        [Time, nil]
      when "ARRAY"
        [Java::JavaSql::Array, nil]
      when "RECORD"
        [Java::JavaSql::Struct, nil]
      when "CURSOR"
        [java.sql.ResultSet, nil]
      else
        [String, nil]
      end
    end
    
    def ruby_value_to_db_value(value, type=nil, metadata={})
      type ||= value.class
      case type.to_s.to_sym
      when :Fixnum, :Bignum, :String, :'Java::JavaLang::Boolean'
        value
      when :BigDecimal
        case value
        when TrueClass
          java_bigdecimal(1)
        when FalseClass
          java_bigdecimal(0)
        else
          java_bigdecimal(value)
        end
      when :Date
        case value
        when DateTime, Date
          java_date(Time.send(plsql.default_timezone, value.year, value.month, value.day, 0, 0, 0))
        else
          java_date(value)
        end
      when :DateTime
        case value
        when DateTime
          java_timestamp(Time.send(plsql.default_timezone, value.year, value.month, value.day, value.hour, value.min, value.sec))
        when Date
          java_timestamp(Time.send(plsql.default_timezone, value.year, value.month, value.day, 0, 0, 0))
        else
          java_timestamp(value)
        end
      when :Time
        java_timestamp(value)
      when :'Java::JavaSql::Clob'
        value
      when :'Java::JavaSql::Blob'
        value
      when :'Java::JavaSql::ResultSet'
        if value
          value.result_set
        end
      else
        value
      end
    end
    
    def db_value_to_ruby_value(value)
      case value
      when Float, BigDecimal
        db_number_to_ruby_number(value)
      when Java::JavaMath::BigDecimal
        value && db_number_to_ruby_number(BigDecimal.new(value.to_s))
      when Java::JavaSql::Date
        if value
          Time.send(plsql.default_timezone, value.year + 1900, value.month + 1, value.date, 0, 0, 0, 0)
        end
      when Java::JavaSql::Timestamp
        if value
          Time.send(plsql.default_timezone, value.year + 1900, value.month + 1, value.date, value.hours, value.minutes, value.seconds, value.nanos / 1000)
        end
      when Java::JavaSql::Clob
        if value.isEmptyLob
          nil
        else
          value.getSubString(1, value.length)
        end
      when Java::JavaSql::Blob
        if value.isEmptyLob
          nil
        else
          String.from_java_bytes(value.getBytes(1, value.length))
        end
      when Java::JavaSql::Array
        begin
          value.getArray.map{|e| db_value_to_ruby_value(e)}
        # If simple array conversion fails, then we are dealing with an array of complex types.
        # This means we have to perform the type-conversion ourselves.
        rescue NativeException
          # Return PGObject-array to be handled externally.
          Cursor.new(self, value.result_set).fetch_all.map {|obj| obj[1]}
        end
      when Java::java.sql.ResultSet
        Cursor.new(self, value)
      else
        value
      end
    end
    
    private
    
    def java_date(value)
      value && Java::java.sql.Date.new(value.year - 1900, value.month - 1, value.day)
    end

    def java_timestamp(value)
      value && Java::java.sql.Timestamp.new(value.year - 1900, value.month - 1, value.day, value.hour, value.min, value.sec, value.usec * 1000)
    end

    def java_bigdecimal(value)
      value && java.math.BigDecimal.new(value.to_s)
    end

    def db_number_to_ruby_number(num)
      num == (num_to_i = num.to_i) ? num_to_i : (num.is_a?(BigDecimal) ? num : BigDecimal.new(num.to_s))
    end
    
  end
  
end
