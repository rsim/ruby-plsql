begin
  require "pg"
rescue LoadError
  # ruby-pg driver is unavailable.
  raise LoadError, "ERROR: ruby-plsql could not load ruby-pg library. Please install pg gem."
end

require "plsql/connection_helpers"

module PLSQL
  class PGConnection < Connection #:nodoc:
    
    include PGConnectionHelper
    
    def self.create_raw(params)
      params.reverse_merge!(:host => "localhost")
      new(PGconn.new(params.inject({}) do |p, (k,v)|
            case k
            when :username
              p[:user] = v
            when :database
              p[:dbname] = v
            else
              p[k] = v unless (k == :dialect || k == :time_zone || k == :adapter)
            end
            p
          end
        ), params
      )
    end
    
    def commit
      # Do nothing as Postgres autocommits.
    end
    
    def rollback
      # Do nothing as Postgres autocommits.
    end
    
    def logoff
      super
      !(raw_connection.finish) rescue nil
    end
    
    def exec(sql, *bindvars)
      raw_connection.exec(sql, *bindvars)
      true
    end
    
    class Cursor #:nodoc:
      include Connection::CursorCommon

      @@open_cursors = []
      @@cursor_idx = 0
      
      attr_reader :stmt_name

      def initialize(conn, sql, params = {})
        @connection = conn
        @sql = sql
        @params = params
        @bindvars = []
        @@open_cursors.push self
      end

      def self.new_from_parse(conn, sql, params = {})
        self.new(conn, sql, params)
      end

      def self.new_from_query(conn, sql, bindvars=[])
        cursor = new_from_parse(conn, sql)
        cursor.exec(*bindvars)
        cursor
      end

      def bind_param(arg, value, metadata)
        type, * = @connection.plsql_to_ruby_data_type(metadata)
        @bindvars[bind_param_index(arg)] = @connection.ruby_value_to_db_value(value, type)
      end
      
      def exec(*bindvars)
        params = @bindvars + (bindvars.inject([]) {|r, v| r << @connection.ruby_value_to_db_value(v)})
        @table = @connection.raw_connection.exec(@sql, params)
        @row_idx = 0
      end

      def [](key)
        column_idx = (key.class == Fixnum)? key: @table.fnumber(key)
        @connection.db_value_to_ruby_value([@table.getvalue(@row_idx, column_idx), @table.ftype(column_idx), @table.fmod(column_idx)])
      end

      def fetch
        row = (@table.values[(@row_idx += 1) - 1] if @table.ntuples > @row_idx)
        row && row.each_with_index.map {|v, i| @connection.db_value_to_ruby_value([v, @table.ftype(i), @table.fmod(i)])}
      end

      def fields
        @fields ||= @table.fields.map{|c| c.downcase.to_sym}
      end

      def close_raw_cursor
        @table.clear if @table
      end

      def close
        while (open_cursor = @@open_cursors.pop) && !open_cursor.equal?(self)
          open_cursor.close_raw_cursor
        end
        close_raw_cursor
      end
      
      private
      
      def bind_param_index(key)
        return key if key.kind_of? Integer
        @params[key]
      end

    end
    
    def parse(sql, params = {})
      Cursor.new_from_parse(self, sql, params)
    end

    def cursor_from_query(sql, bindvars=[], *)
      Cursor.new_from_query(self, sql, bindvars)
    end
    
    module PGBoolean end;

    def plsql_to_ruby_data_type(metadata)
      data_type, data_length = metadata[:data_type], metadata[:data_length]
      case data_type
      when "VARCHAR", "CHAR"
        [String, data_length || 32767]
      when "TEXT"
        [String, nil]
      when "BOOLEAN"
        [PGBoolean, nil]
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
        [Array, nil]
      else
        [String, nil]
      end
    end
    
    # OID's of datatypes from pg_type.h file.
    DATA_TYPE_TO_OID = {
      :boolean => 16,
      :integer => 23,
      :text => 25,
      :char => 1042,
      :varchar => 1043,
      :date => 1082,
      :time => 1083,
      :time_tz => 1266,
      :timestamp => 1114,
      :timestamp_tz => 1184,
      :numeric => 1700
    }
    
    def ruby_value_to_db_value(value, type = nil)
      type ||= value.class
      case type.to_s.to_sym
      when :Fixnum
        {:value => value, :type => DATA_TYPE_TO_OID[:integer]}
      when :Bignum, :BigDecimal, :Float
        {:value =>
            case value
          when TrueClass
            1
          when FalseClass
            0
          else
            value
          end, :type => DATA_TYPE_TO_OID[:numeric]}
      when :PGBoolean
        {:value => value, :type => DATA_TYPE_TO_OID[:boolean]}
      when :String
        {:value => value, :type => DATA_TYPE_TO_OID[:text]}
      when :Time, :Date, :DateTime
        {:value => value, :type => DATA_TYPE_TO_OID[:timestamp_tz]}
      else
        {:value => value, type => DATA_TYPE_TO_OID[:text]}
      end.merge(:format => 0) unless value.nil?
    end
    
    def db_value_to_ruby_value(value)
      type = raw_connection.exec("SELECT format_type($1, $2)", [value[1], value[2]]).getvalue(0, 0).to_sym
      case type
      when :integer, :bigint, :numeric
        # return BigDecimal instead of Float to avoid rounding errors
        value[0].to_s == (num_to_i = value[0].to_i).to_s ? num_to_i : (value[0].is_a?(BigDecimal) ? value[0] : BigDecimal.new(value[0].to_s))
      when :boolean
        value[0] == 't'? true: false
      when :'time with time zone', :'time without time zone', :'timestamp with time zone', :'timestamp without time zone'
        Time.parse(value[0])
      when :date
        Date.parse(value[0])
      else
        value[0]
      end unless value[0].nil?
    end
    
    def database_version
      @database_version ||= (version = raw_connection.server_version.to_s.scan(/(\d)(\d\d)(\d\d)/)[0]) && version.map {|e| e.to_i}
    end
    
  end
  
end