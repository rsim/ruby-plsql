# encoding: utf-8

require 'spec_helper'

describe "Postgres Connection" do
  
  before(:all) do
    @raw_conn = get_connection(:dialect => :postgres)
    @conn = PLSQL::Connection.create(@raw_conn, :dialect => :postgres)
    @conn.set_time_zone
  end
  
  after(:all) do
    @raw_conn.close rescue nil
  end
  
  describe "create and destroy" do
    
    before (:each) do
      @conn = PLSQL::Connection.create(@raw_conn, :dialect => :postgres)
      @conn.set_time_zone
    end
    
    it "should create connection" do
      @conn.raw_connection.should == @raw_conn
    end
    
    unless defined?(JRuby)
      it "should be pg connection" do
        @conn.dialect.should == :postgres
      end
    end
    
    it "should logoff connection" do
      @conn.logoff.should be_true
    end
    
  end
  
  # Ruby 1.8 and 1.9
  unless defined?(JRuby)
    describe "PG data type conversions" do
      it "should translate PL/SQL VARCHAR to Ruby String" do
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => 100).should == [String, 100]
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => nil).should == [String, 32767]
      end

      it "should translate PL/SQL CLOB to Ruby String" do
        pending "Postgres doesn't support CLOB, it uses TEXT instead"
      end

      it "should translate PL/SQL NUMBER to Ruby BigDecimal" do
        @conn.plsql_to_ruby_data_type(:data_type => "NUMERIC", :data_length => 15).should == [BigDecimal, nil]
      end

      it "should translate PL/SQL DATE to Ruby Date" do
        @conn.plsql_to_ruby_data_type(:data_type => "DATE", :data_length => nil).should == [Date, nil]
      end

      it "should translate PL/SQL TIMESTAMP to Ruby DateTime" do
        @conn.plsql_to_ruby_data_type(:data_type => "TIMESTAMP", :data_length => nil).should == [DateTime, nil]
      end

      it "should translate Ruby String value to CLOB when CLOB type specified" do
        pending "Postgres doesn't support CLOB, it uses TEXT instead"
      end

      it "should translate Postgres CLOB value to String" do
        pending "Postgres doesn't support CLOB, it uses TEXT instead"
      end
      
    end

    # JRuby
  else

    describe "JDBC data type conversions" do
      it "should translate PL/SQL VARCHAR to Ruby String" do
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => 100).should == [String, 100]
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => nil).should == [String, 32767]
      end

      it "should translate PL/SQL NUMERIC to Ruby BigDecimal" do
        @conn.plsql_to_ruby_data_type(:data_type => "NUMERIC", :data_length => 15).should == [BigDecimal, nil]
      end
      
      it "should translate PL/SQL DATE to Ruby Date" do
        @conn.plsql_to_ruby_data_type(:data_type => "DATE", :data_length => nil).should == [Date, nil]
      end
      
      it "should translate PL/SQL TIMESTAMP to Ruby DateTime" do
        @conn.plsql_to_ruby_data_type(:data_type => "TIMESTAMP", :data_length => nil).should == [DateTime, nil]
      end
      
      it "should not translate Ruby Fixnum when BigDecimal type specified" do
        @conn.ruby_value_to_db_value(100, BigDecimal).should == java.math.BigDecimal.new(100)
      end
      
      it "should translate Ruby Bignum value to BigDecimal when BigDecimal type specified" do
        big_decimal = @conn.ruby_value_to_db_value(12345678901234567890, BigDecimal)
        big_decimal.should == java.math.BigDecimal.new("12345678901234567890")
      end

      it "should translate Ruby String value to Java::JavaSql::Clob when Java::JavaSql::Clob type specified" do
        pending "Postgres doesn't support CLOB, it uses TEXT instead"
      end

      it "should translate Ruby nil value to empty Java::JavaSql::Clob when Java::JavaSql::Clob type specified" do
        pending "Postgres doesn't support CLOB, it uses TEXT instead"
      end

      it "should translate Oracle BigDecimal integer value to Fixnum" do
        @conn.db_value_to_ruby_value(BigDecimal("100")).should eql(100)
      end
      
      it "should translate Oracle BigDecimal float value to BigDecimal" do
        @conn.db_value_to_ruby_value(BigDecimal("100.11")).should eql(BigDecimal("100.11"))
      end

      it "should translate Postgres CLOB value to String" do
        pending "Postgres doesn't support CLOB, it uses TEXT instead"
      end

      it "should translate empty Postgres CLOB value to nil" do
        pending "Postgres doesn't support CLOB, it uses TEXT instead"
      end

    end

  end
  
  describe "SQL SELECT statements" do
    
    it "should execute SQL statement and return first result" do
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_first("VALUES ('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS'))"
      ).should == ["abc", 123, 123.456, @now]
    end
    
    it "should execute SQL statement and return first result as hash" do
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_hash_first("SELECT 'abc'::varchar a, 123 b, 123.456 c,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS') d"
      ).should == {:a => "abc", :b => 123, :c => 123.456, :d => @now}
    end
    
    # Bind variables are specified differently when using ruby-pg and JDBC driver.
    unless defined?(JRuby)
      it "should execute SQL statement with bind parameters and return first result" do
        @today = Date.parse("2008-05-31")
        @now = Time.local(2008, 05, 31, 23, 22, 11)
        @conn.select_first("VALUES ($1, $2, $3, $4, $5)", 'abc', 123, 123.456, @now, @today
        ).should == ["abc", 123, 123.456, @now, Time.parse(@today.to_s)]
      end
    else
      it "should execute SQL statement with bind parameters and return first result" do
        @today = Date.parse("2008-05-31")
        @now = Time.local(2008, 05, 31, 23, 22, 11)
        @conn.select_first("VALUES (?, ?, ?, ?::timestamp, ?::date)", 'abc', 123, 123.456, @now, @today
        ).should == ["abc", 123, 123.456, @now, Time.parse(@today.to_s)]
      end
    end
    
    it "should execute SQL statement with NULL values and return first result" do
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_first("VALUES (NULL, 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS'))"
      ).should == [nil, 123, 123.456, @now]
    end
    
    if defined?(JRuby)

      it "should execute SQL statement with NULL values as bind parameters and return first result" do
        @today = Date.parse("2008-05-31")
        @now = Time.local(2008, 05, 31, 23, 22, 11)
        @conn.select_first("VALUES (?, ?, ?, ?::timestamp, ?::date)",
          nil, 123, 123.456, @now, @today).should == [nil, 123, 123.456, @now, Time.parse(@today.to_s)]
      end
    
    end
    
    it "should execute SQL statement and return all results" do
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_all("VALUES ('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS'))
        UNION ALL VALUES('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS'))"
      ).should == [["abc", 123, 123.456, @now], ["abc", 123, 123.456, @now]]
    end
    
    it "should execute SQL statement and return all results as hash" do
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_hash_all("SELECT 'abc' a, 123 b, 123.456 c,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS') d
        UNION ALL SELECT 'def' a, 123 b, 123.456 c,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS') d"
      ).should == [{:a => "abc", :b => 123, :c => 123.456, :d => @now}, {:a => "def", :b => 123, :c => 123.456, :d => @now}]
    end
    
    # Bind variables are specified differently when using ruby-pg and JDBC driver.
    unless defined?(JRuby)
      it "should execute SQL statement with bind parameters and return all results" do
        @now = Time.local(2008, 05, 31, 23, 22, 11)
        @conn.select_all("VALUES($1, $2, $3, $4) UNION ALL VAlUES($5, $6, $7, $8)",
          'abc', 123, 123.456, @now, 'abc', 123, 123.456, @now).should == [["abc", 123, 123.456, @now], ["abc", 123, 123.456, @now]]
      end
    else
      it "should execute SQL statement with bind parameters and return all results" do
        @now = Time.local(2008, 05, 31, 23, 22, 11)
        @conn.select_all("VALUES(?, ?, ?, ?::timestamp) UNION ALL VAlUES(?, ?, ?, ?::timestamp)",
          'abc', 123, 123.456, @now, 'abc', 123, 123.456, @now).should == [["abc", 123, 123.456, @now], ["abc", 123, 123.456, @now]]
      end
    end
    
    it "should execute SQL statement and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("VALUES ('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS'))
        UNION ALL VALUES('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS'))"
      ) do |r|
        r.should == ["abc", 123, 123.456, @now]
      end.should == 2
    end
    
    # Bind variables are specified differently when using ruby-pg and JDBC driver.
    unless defined?(JRuby)
      it "should execute SQL statement with bind parameters and yield all results in block" do
        @now = Time.local(2008, 05, 31, 23, 22, 11)
        @conn.select_all("VALUES ($1, $2, $3, $4) UNION ALL VALUES($5, $6, $7, $8)",
          'abc', 123, 123.456, @now, 'abc', 123, 123.456, @now) do |r|
          r.should == ["abc", 123, 123.456, @now]
        end.should == 2
      end
    else
      it "should execute SQL statement with bind parameters and yield all results in block" do
        @now = Time.local(2008, 05, 31, 23, 22, 11)
        @conn.select_all("VALUES (?, ?, ?, ?::timestamp) UNION ALL VALUES(?, ?, ?, ?::timestamp)",
          'abc', 123, 123.456, @now, 'abc', 123, 123.456, @now) do |r|
          r.should == ["abc", 123, 123.456, @now]
        end.should == 2
      end
    end
    
  end
  
  describe "PL/SQL procedures" do
    
    before(:all) do
      @random = rand(1000)
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      sql = <<-SQL
        CREATE OR REPLACE FUNCTION test_add_random(INOUT p_number numeric, INOUT p_varchar varchar, INOUT p_date timestamp with time zone)
          RETURNS RECORD
        AS $$
        BEGIN
          p_number := p_number + #{@random};
          RETURN;
        END;
        $$ LANGUAGE plpgsql;
      SQL
      @conn.exec(sql).should be_true
    end
    
    after(:all) do
      @conn.exec "DROP FUNCTION test_add_random(numeric, varchar, timestamp with time zone);"
    end
    
    unless defined?(JRuby)
      it "should parse PL/SQL procedure call and bind parameters and exec and get bind parameter value" do
        cursor = @conn.parse("SELECT (test_add_random($1, $2, $3)).*;")
        cursor.bind_param(0, 100, :data_type => 'NUMERIC')
        cursor.bind_param(1, "abc", :data_type => 'VARCHAR')
        cursor.bind_param(2, @now, :data_type => 'TIMESTAMP WITH TIME ZONE')
        cursor.exec
        cursor["p_number"].should == @random + 100
        cursor["p_varchar"].should == "abc"
        cursor["p_date"].should == @now
        cursor.close.should be_nil
      end
    else
      it "should parse PL/SQL procedure call and bind parameters and exec and get bind parameter value" do
        cursor = @conn.parse("{call test_add_random(?, ?, ?::timestamp with time zone)}")
        cursor.bind_param(1, 100, :data_type => 'NUMERIC', :in_out => "IN/OUT")
        cursor.bind_param(2, "abc", :data_type => 'VARCHAR', :in_out => "IN/OUT")
        cursor.bind_param(3, @now, :data_type => 'TIMESTAMP WITH TIME ZONE', :in_out => "IN/OUT")
        cursor.exec
        cursor[1].should == @random + 100
        cursor[2].should == "abc"
        cursor[3].should == @now
        cursor.close.should be_nil
      end
    end
    
  end
  
  describe "commit and rollback" do
    it "should do commit" do
      pending "Postgres autocommits by default"
    end
    
    it "should do rollback" do
      pending "Postgres autocommits by default"
    end
    
    it "should do commit and rollback should not undo commited transaction" do
      pending "Postgres autocommits by default"
    end
  end

  describe "prefetch rows" do
    it "should set prefetch rows for connection" do
      pending "can't prefetch using Postgres native driver"
    end
    
    it "should fetch just one row when using select_first" do
      pending "can't prefetch using Postgres native driver"
    end
  end
  
  describe "describe synonym" do
    it "should describe local synonym" do
      pending "synonyms not supported in Postgres"
    end
    
    it "should return nil on non-existing synonym" do
      pending "synonyms not supported in Postgres"
    end
    
    it "should describe public synonym" do
      pending "synonyms not supported in Postgres"
    end
  end
  
  describe "session information" do
    it "should get database version" do
      # using Postgres version 9.0.3 for unit tests
      @conn.database_version.should == PG_DATABASE_VERSION.split('.').map{|n| n.to_i}
    end

    it "should get session ID" do
      @conn.session_id.should == @conn.select_first("SELECT pg_backend_pid()")[0].to_i
    end
  end
  
  describe "drop ruby temporary tables" do
    after(:all) do
      @conn.drop_all_ruby_temporary_tables
    end

    it "should drop all ruby temporary tables" do
      tmp_table = "ruby_111_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should_not raise_error
      @conn.drop_all_ruby_temporary_tables
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should raise_error(/relation "#{tmp_table}" does not exist/)
    end

    it "should drop current session ruby temporary tables" do
      tmp_table = "ruby_#{@conn.session_id}_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should_not raise_error
      @conn.drop_session_ruby_temporary_tables
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should raise_error(/relation "#{tmp_table}" does not exist/)
    end

    it "should not drop other session ruby temporary tables" do
      tmp_table = "ruby_#{@conn.session_id+1}_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should_not raise_error
      @conn.drop_session_ruby_temporary_tables
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should_not raise_error
    end

  end
  
  describe "logoff" do
    before(:each) do
      # restore connection before each test
      reconnect_connection
    end

    after(:all) do
      @conn.exec "DROP TABLE test_dummy_table" rescue nil
    end

    def reconnect_connection
      @raw_conn = get_connection(:dialect => :postgres)
      @conn = PLSQL::Connection.create(@raw_conn, :dialect => :postgres)
      @conn.set_time_zone
    end

    it "should drop current session ruby temporary tables" do
      tmp_table = "ruby_#{@conn.session_id}_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should_not raise_error
      @conn.logoff
      reconnect_connection
      lambda { @conn.select_first("SELECT * FROM #{tmp_table}") }.should raise_error(/relation "#{tmp_table}" does not exist/)
    end

    it "should rollback any uncommited transactions" do
      pending "Postgres autocommits by default"
    end

  end
  
end