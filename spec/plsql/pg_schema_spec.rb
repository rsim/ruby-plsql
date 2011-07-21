require 'spec_helper'

describe "Schema" do
  
  it "should create Schema object" do
    plsql.class.should == PLSQL::Schema
  end
  
end

describe "Postgres Schema connection" do
  
  before(:each) do
    @conn = get_connection(:dialect => :postgres)
  end

  after(:each) do
    unless defined?(JRuby)
      @conn.finish
    else
      @conn.close
    end
  end

  it "should connect to test database using connection alias" do
    plsql(:pg).connection = PLSQL::Connection.create(@conn, :dialect => :postgres)
    plsql(:pg).connection.raw_connection.should == @conn
  end
  
  it "should return schema name" do
    plsql(:pg).connection = PLSQL::Connection.create(@conn, :dialect => :postgres)
    plsql(:pg).schema_name.should == DATABASE_USERS_AND_PASSWORDS[0][0].upcase
  end

  it "should return new schema name after reconnection" do
    plsql(:pg).connection = PLSQL::Connection.create(@conn, :dialect => :postgres)
    plsql(:pg).schema_name.should == DATABASE_USERS_AND_PASSWORDS[0][0].upcase
    plsql(:pg).connection = PLSQL::Connection.create(get_connection(:user_number => 1, :dialect => :postgres), :dialect => :postgres)
    plsql(:pg).schema_name.should == DATABASE_USERS_AND_PASSWORDS[1][0].upcase
  end

  it "should return nil schema name if not connected" do
    plsql(:xxx).schema_name.should be_nil
  end

end

describe "Postgres Connection with connect!" do

  before(:all) do
    @username, @password = DATABASE_USERS_AND_PASSWORDS[0]
    @database = PG_DATABASE_NAME
    @host = DATABASE_HOST
    @port = PG_DATABASE_PORT
  end

  after(:each) do
    plsql(:pg).logoff if plsql(:pg).connection
  end

  it "should connect with username, password and database alias" do
    plsql(:pg).connect! @username, @password, @database, :dialect => :postgres
    plsql(:pg).connection.should_not be_nil
    plsql(:pg).schema_name.should == @username.upcase
  end

  it "should connect with username, password, host, port and database name" do
    plsql(:pg).connect! @username, @password, :host => @host, :port => @port, :database => @database, :dialect => :postgres
    plsql(:pg).connection.should_not be_nil
    plsql(:pg).schema_name.should == @username.upcase
  end

  it "should connect with username, password, host, database name and default port" do
    pending "Non-default port used for test database" unless @port == 1521
    plsql(:pg).connect! @username, @password, :host => @host, :database => @database, :dialect => :postgres
    plsql(:pg).connection.should_not be_nil
    plsql(:pg).schema_name.should == @username.upcase
  end

  it "should not connect with wrong port number" do
    lambda {
      plsql(:pg).connect! @username, @password, :host => @host, :port => 9999, :database => @database, :dialect => :postgres
    }.should raise_error
  end

  it "should connect with one Hash parameter" do
    plsql(:pg).connect! :username => @username, :password => @password, :database => @database, :dialect => :postgres
    plsql(:pg).connection.should_not be_nil
    plsql(:pg).schema_name.should == @username.upcase
  end

  it "should set session time zone from TZ environment variable" do
    plsql(:pg).connect! @username, @password, @database, :dialect => :postgres
    plsql(:pg).connection.time_zone.should == ENV['TZ']
  end

  it "should set session time zone from :time_zone parameter" do
    plsql(:pg).connect! :username => @username, :password => @password, :database => @database, :dialect => :postgres, :time_zone => 'EET'
    plsql(:pg).connection.time_zone.should == 'EET'
  end

end


describe "Postgres Named Schema" do
  before(:all) do
    @conn = get_connection(:dialect => :postgres)
    plsql(:pg).connection = PLSQL::Connection.create(@conn, :dialect => :postgres)
  end

  after(:all) do
    plsql(:pg).connection.logoff
  end

  it "should find existing schema" do
    plsql(:pg).hr.class.should == PLSQL::Schema
  end

  it "should have the same connection as default schema" do
    plsql(:pg).hr.connection.raw_connection.should == @conn
  end

  it "should return schema name" do
    plsql(:pg).hr.schema_name.should == 'HR'
  end

  it "should not find named schema if specified twice" do
    lambda { plsql(:pg).hr.hr }.should raise_error(ArgumentError)
  end

end

describe "Postgres Schema commit and rollback" do
  
  before(:all) do
    #plsql.connection = @conn = get_connection
    #plsql.connection.autocommit = false
    #plsql.execute "CREATE TABLE test_commit (dummy VARCHAR(100))"
    #@data = {:dummy => 'test'}
    #@data2 = {:dummy => 'test2'}
  end

  after(:all) do
    #plsql.execute "DROP TABLE test_commit"
    #plsql.logoff
  end

  after(:each) do
    #plsql.test_commit.delete
    #plsql.commit
  end

  it "should do commit" do
    pending "need to implement this"
    #plsql.test_commit.insert @data
    #plsql.commit
    #plsql.test_commit.first.should == @data
  end

  it "should do rollback" do
    pending "need to implement this"
    #plsql.test_commit.insert @data
    #plsql.rollback
    #plsql.test_commit.first.should be_nil
  end

  it "should create savepoint and rollback to savepoint" do
    pending "need to implement this"
    #plsql.test_commit.insert @data
    #plsql.savepoint 'test'
    #plsql.test_commit.insert @data2
    #plsql.test_commit.all.should == [@data, @data2]
    #plsql.rollback_to 'test'
    #plsql.test_commit.all.should == [@data]
  end

end

describe "Postgres ActiveRecord connection" do
  before(:all) do
    ActiveRecord::Base.establish_connection(PG_CONNECTION_PARAMS)
  end

  before(:each) do
    plsql(:pg).connection = PLSQL::Connection.create(nil, :ar_class => ActiveRecord::Base, :dialect => :postgres)
  end

  it "should connect to test database" do
    unless defined?(JRuby)
      plsql(:pg).connection.is_a?(PLSQL::PGConnection).should be_true
    else
      plsql(:pg).connection.is_a?(PLSQL::JDBCPGConnection).should be_true
    end
  end

  it "should return schema name" do
    plsql(:pg).schema_name.should == 'HR'
  end

  it "should use ActiveRecord::Base.default_timezone as default" do
    ActiveRecord::Base.default_timezone = :utc
    plsql(:pg).default_timezone.should == :utc
  end

  it "should have the same connection as default schema" do
    plsql(:pg).hr.connection.should == plsql(:pg).connection
  end
end if defined?(ActiveRecord) && !defined?(JRuby)

describe "Postgres Output logging" do

  before(:all) do
    plsql(:pg).connection = PLSQL::Connection.create(get_connection(:dialect => :postgres), :dialect => :postgres)
    plsql(:pg).execute <<-SQL
      CREATE OR REPLACE FUNCTION test_dbms_output(p_string varchar)
        RETURNS VOID
      AS $$
      BEGIN
        RAISE NOTICE '%', $1;
      END;
      $$ LANGUAGE 'plpgsql';
    SQL
    plsql(:pg).execute <<-SQL
      CREATE OR REPLACE FUNCTION test_dbms_output_large(p_string varchar, p_times integer)
        RETURNS VOID
      AS $$
      BEGIN
        FOR i IN 1..$2 LOOP
          RAISE NOTICE '%', $1;
        END LOOP;
      END;
      $$ LANGUAGE 'plpgsql';
    SQL
    @buffer = StringIO.new
  end

  before(:each) do
    @buffer.rewind
    @buffer.reopen
  end

  after(:all) do
    plsql(:pg).dbms_output_stream=nil
    plsql(:pg).execute "DROP FUNCTION test_dbms_output(varchar)"
    plsql(:pg).execute "DROP FUNCTION test_dbms_output_large(varchar, integer)"
    plsql(:pg).logoff
  end

  describe "with standard connection" do
    before(:all) do
      plsql(:pg).dbms_output_stream = @buffer
    end

    before(:each) do
      plsql(:pg).dbms_output_buffer_size = nil
    end

    it "should log output to specified stream" do
      plsql(:pg).test_dbms_output("test_dbms_output")
      if defined?(JRuby)
        @buffer.string.should == "NOTICE: test_dbms_output\n"
      else
        @buffer.string.should == "NOTICE:  test_dbms_output\n"
      end
    end

    it "should not log output to stream when output is disabled" do
      plsql(:pg).test_dbms_output("enabled")
      plsql(:pg).dbms_output_stream = nil
      plsql(:pg).test_dbms_output("disabled")
      plsql(:pg).dbms_output_stream = @buffer
      plsql(:pg).test_dbms_output("enabled again")
      if defined?(JRuby)
        @buffer.string.should == "NOTICE: enabled\nNOTICE: enabled again\n"
      else
        @buffer.string.should == "NOTICE:  enabled\nNOTICE:  enabled again\n"
      end
    end

    it "should log 20_000 character output with default buffer size" do
      times = 2_000
      plsql(:pg).test_dbms_output_large("1234567890", times)
      if defined?(JRuby)
        @buffer.string.should == "NOTICE: 1234567890\n" * times
      else
        @buffer.string.should == "NOTICE:  1234567890\n" * times
      end
    end

    it "should log 100_000 character output with specified buffer size" do
      times = 10_000
      plsql(:pg).dbms_output_buffer_size = 10 * times
      plsql(:pg).test_dbms_output_large("1234567890", times)
      if defined?(JRuby)
        @buffer.string.should == "NOTICE: 1234567890\n" * times
      else
        @buffer.string.should == "NOTICE:  1234567890\n" * times
      end
    end

    it "should log output when database version is less than 10.2" do
      plsql(:pg).connection.stub!(:database_version).and_return([9, 2, 0, 0])
      times = 2_000
      plsql(:pg).test_dbms_output_large("1234567890", times)
      if defined?(JRuby)
        @buffer.string.should == "NOTICE: 1234567890\n" * times
      else
        @buffer.string.should == "NOTICE:  1234567890\n" * times
      end
    end

    it "should log output when calling procedure with schema prefix" do
      plsql(:pg).hr.test_dbms_output("test_dbms_output")
      if defined?(JRuby)
        @buffer.string.should == "NOTICE: test_dbms_output\n"
      else
        @buffer.string.should == "NOTICE:  test_dbms_output\n"
      end
    end

  end

  describe "with Activerecord connection" do
    
    before(:all) do
      ActiveRecord::Base.establish_connection(PG_CONNECTION_PARAMS)
      plsql(:pg_ar).activerecord_class = ActiveRecord::Base
      plsql(:pg_ar).connection = PLSQL::Connection.create(nil, :ar_class => ActiveRecord::Base, :dialect => :postgres)
      plsql(:pg_ar).dbms_output_stream = @buffer
    end

    it "should log output to specified stream" do
      plsql(:pg_ar).test_dbms_output("test_dbms_output")
      @buffer.string.should == "NOTICE:  test_dbms_output\n"
    end

    it "should log output after reconnection" do
      ActiveRecord::Base.connection.reconnect!
      plsql(:pg_ar).test_dbms_output("after reconnection")
      @buffer.string.should == "NOTICE:  after reconnection\n"
    end

  end if defined?(ActiveRecord) && !defined?(JRuby)

end
