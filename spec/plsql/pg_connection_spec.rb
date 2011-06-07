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
  
  describe "SQL SELECT statements" do
    
    it "should execute SQL statement and return first result" do
      @now = Time.local(2008, 05 ,31, 23, 22, 11)
      @conn.select_first("VALUES ('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS'))"
      ).should == ["abc", 123, 123.456, @now]
    end
   
    it "should execute SQL statement and return first result as hash" do
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_hash_first("SELECT 'abc' a, 123 b, 123.456 c,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS') d"
      ).should == {:a => "abc", :b => 123, :c => 123.456, :d => @now}
    end
    
    it "should execute SQL statement with bind parameters and return first result" do
      @today = Date.parse("2008-05-31")
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_first("VALUES ($1, $2, $3, $4, $5)", 'abc', 123, 123.456, @now, @today
      ).should == ["abc", 123, 123.456, @now, Time.parse(@today.to_s)]
    end
    
    it "should execute SQL statement with NULL values and return first result" do
      @now = Time.local(2008, 05, 31, 23, 22, 11)
      @conn.select_first("VALUES (NULL, 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS'))"
      ).should == [nil, 123, 123.456, @now]
    end
    
    if defined?(JRuby)
      it "should execute SQL statement with NULL values as bind parameters and return first result" do
        fail "Need to implement this."
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
    
    it "should execute SQL statement with bind parameters and return all results" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("VALUES($1, $2, $3, $4) UNION ALL VAlUES($5, $6, $7, $8)",
        'abc', 123, 123.456, @now, 'abc', 123, 123.456, @now).should == [["abc", 123, 123.456, @now], ["abc", 123, 123.456, @now]]
    end
    
    it "should execute SQL statement and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("VALUES ('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS'))
        UNION ALL VALUES('abc', 123, 123.456,
        to_timestamp('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS'))"
      ) do |r|
        r.should == ["abc", 123, 123.456, @now]
      end.should == 2
    end
    
    it "should execute SQL statement with bind parameters and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("VALUES ($1, $2, $3, $4) UNION ALL VALUES($5, $6, $7, $8)",
        'abc', 123, 123.456, @now, 'abc', 123, 123.456, @now) do |r|
        r.should == ["abc", 123, 123.456, @now]
      end.should == 2
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
    
    it "should parse PL/SQL procedure call and bind parameters and exec and get bind parameter value" do
      cursor = @conn.parse("SELECT (test_add_random($1, $2, $3)).*;")
      cursor.bind_param(0, 100, :numeric)
      cursor.bind_param(1, "abc", :varchar)
      cursor.bind_param(2, @now, :timestamp_tz)
      cursor.exec
      cursor["p_number"].should == @random + 100
      cursor["p_varchar"].should == "abc"
      cursor["p_date"].should == @now
      cursor.close.should be_nil
    end
    
  end
  
  describe "commit and rollback" do
    # Need to think about implementation as Postgres autocommits by default.
  end

  describe "prefetch rows" do
    # Can't prefetch using Postgres native driver.
  end
  
  describe "describe synonym" do
    # Synonyms not supported in Postgres.
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
      # Need to think about implementation as Postgres autocommits by default.
    end

  end
  
end