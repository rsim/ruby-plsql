begin
  require "pg"
rescue LoadError
  # ruby-pg driver is unavailable.
  raise LoadError, "ERROR: ruby-plsql could not load ruby-pg library. Please install pg gem."
end

require_relative "helpers"

module PLSQL
  class PGConnection < Connection #:nodoc:
    
    # OID's of datatypes from pg_type.h file.
    @@datatype_to_oid = {
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
    
    include PGConnectionHelper
    
    def self.create_raw(params)
      new(PGconn.new(params.inject({}) do |p, (k,v)|
            case k
            when :username
              p[:user] = v
            when :database
              p[:dbname] = v
            else p[k] = v
            end
            p
          end
        )
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

      def initialize(conn, sql)
        @connection = conn
        @sql = sql
        @bindvars = []
        @@open_cursors.push self
      end

      def self.new_from_parse(conn, sql)
        self.new(conn, sql)
      end

      def self.new_from_query(conn, sql, bindvars=[])
        cursor = new_from_parse(conn, sql)
        cursor.exec(*bindvars)
        cursor
      end

      def bind_param(arg, value, type = nil)
        @bindvars[arg] = @connection.ruby_value_to_pg_value(value, type)
      end
      
      def exec(*bindvars)
        params = @bindvars + (bindvars.inject([]) {|r, v| r << @connection.ruby_value_to_pg_value(v)})
        @table = @connection.raw_connection.exec(@sql, params)
        @row_idx = 0
      end

      def [](key)
        column_idx = @table.fnumber(key)
        @connection.pg_value_to_ruby_value([@table[@row_idx][key], @table.ftype(column_idx), @table.fmod(column_idx)])
      end

      def fetch
        row = (@table.values[(@row_idx += 1) - 1] if @table.ntuples > @row_idx)
        row && row.each_with_index.map {|v, i| @connection.pg_value_to_ruby_value([v, @table.ftype(i), @table.fmod(i)])}
      end

      def fields
        @fields ||= @table.fields.map{|c| c.downcase.to_sym}
      end

      def close_raw_cursor
        @table.clear
      end

      def close
        while (open_cursor = @@open_cursors.pop) && !open_cursor.equal?(self)
          open_cursor.close_raw_cursor
        end
        close_raw_cursor
      end

    end
    
    def parse(sql)
      Cursor.new_from_parse(self, sql)
    end

    def cursor_from_query(sql, bindvars=[], *)
      Cursor.new_from_query(self, sql, bindvars)
    end
    
    def ruby_value_to_pg_value(value, type = nil)
      {value: value, format: 0, type:
          (@@datatype_to_oid[type] ||
            case value.class.to_s.to_sym
          when :Fixnum
            @@datatype_to_oid[:integer]
          when :BigDecimal, :Float
            @@datatype_to_oid[:numeric]
          when :String
            @@datatype_to_oid[:text]
          when :Time, :Date, :DateTime
            @@datatype_to_oid[:timestamp_tz]
          else
            @@datatype_to_oid[:text]
          end)
      }
      
    end
    
    def pg_value_to_ruby_value(value)
      type = raw_connection.exec("SELECT format_type($1,$2)", [value[1], value[2]]).getvalue(0,0).to_sym
      case type
      when :integer, :numeric
        # return BigDecimal instead of Float to avoid rounding errors
        value[0] == (num_to_i = value[0].to_i).to_s ? num_to_i : BigDecimal.new(value[0])
      when :"timestamp with time zone", :"timestamp without time zone",
          :"time with time zone", :"time without time zone"
        Time.parse(value[0])
      when :date
        Date.parse(value[0])
      else
        value[0]
      end
    end
    
    def database_version
      @database_version ||= (version = raw_connection.server_version.to_s.scan(/(\d)(\d\d)(\d\d)/)[0]) && version.map {|e| e.to_i}
    end
    
  end
  
end