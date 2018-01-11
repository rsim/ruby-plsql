begin
  require "oci8"
rescue LoadError
  # OCI8 driver is unavailable.
  msg = $!.to_s
  if /-- oci8$/ =~ msg
    # ruby-oci8 is not installed.
    # MRI <= 1.9.2, Rubinius, JRuby:
    #   no such file to load -- oci8
    # MRI >= 1.9.3:
    #   cannot load such file -- oci8
    msg = "Please install ruby-oci8 gem."
  end
  raise LoadError, "ERROR: ruby-plsql could not load ruby-oci8 library. #{msg}"
end

require "plsql/oci8_patches"

# check ruby-oci8 version
required_oci8_version = [2, 0, 3]
oci8_version_ints = OCI8::VERSION.scan(/\d+/).map{|s| s.to_i}
if (oci8_version_ints <=> required_oci8_version) < 0
  raise LoadError, "ERROR: ruby-oci8 version #{OCI8::VERSION} is too old. Please install ruby-oci8 version #{required_oci8_version.join('.')} or later."
end

module PLSQL
  class OCIConnection < Connection #:nodoc:

    def self.create_raw(params)
      connection_string = if params[:host]
        "//#{params[:host]}:#{params[:port]||1521}/#{params[:database]}"
      else
        params[:database]
      end
      new(OCI8.new(params[:username], params[:password], connection_string))
    end

    def logoff
      super
      raw_connection.logoff
    end

    def commit
      raw_connection.commit
    end

    def rollback
      raw_connection.rollback
    end

    def autocommit?
      raw_connection.autocommit?
    end

    def autocommit=(value)
      raw_connection.autocommit = value
    end

    def prefetch_rows=(value)
      raw_connection.prefetch_rows = value
    end

    def exec(sql, *bindvars)
      raw_connection.exec(sql, *bindvars)
      true
    end

    class Cursor #:nodoc:
      include Connection::CursorCommon

      attr_reader :raw_cursor

      # stack of open cursors per thread
      def self.open_cursors
        Thread.current[:plsql_oci_cursor_stack] ||= []
      end

      def initialize(conn, raw_cursor)
        @connection = conn
        @raw_cursor = raw_cursor
        self.class.open_cursors.push self
      end

      def self.new_from_parse(conn, sql)
        raw_cursor = conn.raw_connection.parse(sql)
        self.new(conn, raw_cursor)
      end

      def self.new_from_query(conn, sql, bindvars=[], options={})
        cursor = new_from_parse(conn, sql)
        if prefetch_rows = options[:prefetch_rows]
          cursor.prefetch_rows = prefetch_rows
        end
        cursor.exec(*bindvars)
        cursor
      end

      def prefetch_rows=(value)
        @raw_cursor.prefetch_rows = value
      end

      def bind_param(arg, value, metadata)
        type, length = @connection.plsql_to_ruby_data_type(metadata)
        ora_value = @connection.ruby_value_to_ora_value(value, type)
        @raw_cursor.bind_param(arg, ora_value, type, length)
      end

      def exec(*bindvars)
        @raw_cursor.exec(*bindvars)
      end

      def [](key)
        @connection.ora_value_to_ruby_value(@raw_cursor[key])
      end

      def fetch
        row = @raw_cursor.fetch
        row && row.map{|v| @connection.ora_value_to_ruby_value(v)}
      end

      def fields
        @fields ||= @raw_cursor.get_col_names.map{|c| c.downcase.to_sym}
      end

      def close_raw_cursor
        @raw_cursor.close
      end

      def close
        # close all cursors that were created after this one
        while (open_cursor = self.class.open_cursors.pop) && !open_cursor.equal?(self)
          open_cursor.close_raw_cursor
        end
        close_raw_cursor
      end

    end

    def parse(sql)
      Cursor.new_from_parse(self, sql)
    end

    def cursor_from_query(sql, bindvars=[], options={})
      Cursor.new_from_query(self, sql, bindvars, options)
    end

    def plsql_to_ruby_data_type(metadata)
      data_type, data_length = metadata[:data_type], metadata[:data_length]
      case data_type
      when "VARCHAR", "VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"
        [String, data_length || 32767]
      when "CLOB", "NCLOB"
        [OCI8::CLOB, nil]
      when "BLOB"
        [OCI8::BLOB, nil]
      when "NUMBER", "NATURAL", "NATURALN", "POSITIVE", "POSITIVEN", "SIGNTYPE", "SIMPLE_INTEGER", "PLS_INTEGER", "BINARY_INTEGER"
        [OraNumber, nil]
      when "DATE"
        [DateTime, nil]
      when "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"
        [Time, nil]
      when "TABLE", "VARRAY", "OBJECT", "XMLTYPE"
        # create Ruby class for collection
        klass = OCI8::Object::Base.get_class_by_typename(metadata[:sql_type_name])
        unless klass
          klass = Class.new(OCI8::Object::Base)
          klass.set_typename metadata[:sql_type_name]
        end
        [klass, nil]
      when "REF CURSOR"
        [OCI8::Cursor]
      else
        [String, 32767]
      end
    end

    def ruby_value_to_ora_value(value, type=nil)
      type ||= value.class
      case type.to_s.to_sym
      when :Integer, :BigDecimal, :String
        value
      when :OraNumber
        # pass parameters as OraNumber to avoid rounding errors
        case value
        when BigDecimal
          OraNumber.new(value.to_s('F'))
        when TrueClass
          OraNumber.new(1)
        when FalseClass
          OraNumber.new(0)
        else
          value
        end
      when :DateTime
        case value
        when Time
          ::DateTime.civil(value.year, value.month, value.day, value.hour, value.min, value.sec, Rational(value.utc_offset, 86400))
        when DateTime
          value
        when Date
          ::DateTime.civil(value.year, value.month, value.day, 0, 0, 0, 0)
        else
          value
        end
      when :"OCI8::CLOB", :"OCI8::BLOB"
        # ruby-oci8 cannot create CLOB/BLOB from ''
        value.to_s.length > 0 ? type.new(raw_oci_connection, value) : nil
      when :"OCI8::Cursor"
        value && value.raw_cursor
      else
        # collections and object types
        if type.superclass == OCI8::Object::Base
          return nil if value.nil?
          tdo = raw_oci_connection.get_tdo_by_class(type)
          if tdo.is_collection?
            raise ArgumentError, "You should pass Array value for collection type parameter" unless value.is_a?(Array)
            elem_list = value.map do |elem|
              if (attr_tdo = tdo.coll_attr.typeinfo)
                attr_type, _ = plsql_to_ruby_data_type(:data_type => 'OBJECT', :sql_type_name => attr_tdo.typename)
              else
                attr_type = elem.class
              end
              ruby_value_to_ora_value(elem, attr_type)
            end
            # construct collection value
            # TODO: change setting instance variable to appropriate ruby-oci8 method call when available
            collection = type.new(raw_oci_connection)
            collection.instance_variable_set('@attributes', elem_list)
            collection
          else # object type
            raise ArgumentError, "You should pass Hash value for object type parameter" unless value.is_a?(Hash)
            object_attrs = value.dup
            object_attrs.keys.each do |key|
              raise ArgumentError, "Wrong object type field passed to PL/SQL procedure" unless (attr = tdo.attr_getters[key])
              case attr.datatype
              when OCI8::TDO::ATTR_NAMED_TYPE, OCI8::TDO::ATTR_NAMED_COLLECTION
                # nested object type or collection
                attr_type, _ = plsql_to_ruby_data_type(:data_type => 'OBJECT', :sql_type_name => attr.typeinfo.typename)
                object_attrs[key] = ruby_value_to_ora_value(object_attrs[key], attr_type)
              end
            end
            type.new(raw_oci_connection, object_attrs)
          end
        # all other cases
        else
          value
        end
      end
    end

    def ora_value_to_ruby_value(value)
      case value
      when Float, OraNumber, BigDecimal
        ora_number_to_ruby_number(value)
      when DateTime, OraDate
        ora_date_to_ruby_date(value)
      when OCI8::LOB
        if value.available?
          value.rewind
          value.read
        else
          nil
        end
      when OCI8::Object::Base
        tdo = raw_oci_connection.get_tdo_by_class(value.class)
        if tdo.is_collection?
          value.to_ary.map{|e| ora_value_to_ruby_value(e)}
        else # object type
          tdo.attributes.inject({}) do |hash, attr|
            hash[attr.name] = ora_value_to_ruby_value(value.instance_variable_get(:@attributes)[attr.name])
            hash
          end
        end
      when OCI8::Cursor
        Cursor.new(self, value)
      else
        value
      end
    end

    def describe_synonym(schema_name, synonym_name)
      if schema_name == 'PUBLIC'
        full_name = synonym_name.to_s
      else
        full_name = "#{schema_name}.#{synonym_name}"
      end
      metadata = raw_connection.describe_synonym(full_name)
      [metadata.schema_name, metadata.name]
    rescue OCIError
      nil
    end

    def database_version
      @database_version ||= (version = raw_connection.oracle_server_version) &&
        [version.major, version.minor, version.update, version.patch]
    end

    private

    def raw_oci_connection
      if raw_connection.is_a? OCI8
        raw_connection
      # ActiveRecord Oracle enhanced adapter puts OCI8EnhancedAutoRecover wrapper around OCI8
      # in this case we need to pass original OCI8 connection
      else
        raw_connection.instance_variable_get(:@connection)
      end
    end

    def ora_number_to_ruby_number(num)
      # return BigDecimal instead of Float to avoid rounding errors
      num == (num_to_i = num.to_i) ? num_to_i : (num.is_a?(BigDecimal) ? num : BigDecimal.new(num.to_s))
    end

    def ora_date_to_ruby_date(val)
      case val
      when DateTime
        # similar implementation as in oracle_enhanced adapter
        begin
          Time.send(plsql.default_timezone, val.year, val.month, val.day, val.hour, val.min, val.sec)
        rescue
          offset = plsql.default_timezone.to_sym == :local ? plsql.local_timezone_offset : 0
          DateTime.civil(val.year, val.month, val.day, val.hour, val.min, val.sec, offset)
        end
      when OraDate
        # similar implementation as in oracle_enhanced adapter
        begin
          Time.send(plsql.default_timezone, val.year, val.month, val.day, val.hour, val.minute, val.second)
        rescue
          offset = plsql.default_timezone.to_sym == :local ? plsql.local_timezone_offset : 0
          DateTime.civil(val.year, val.month, val.day, val.hour, val.minute, val.second, offset)
        end
      else
        val
      end
    end

  end

end
