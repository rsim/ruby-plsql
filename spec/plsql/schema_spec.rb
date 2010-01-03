require File.dirname(__FILE__) + '/../spec_helper'

describe "Schema" do
  
  it "should create Schema object" do
    plsql.class.should == PLSQL::Schema
  end
  
end

describe "Schema connection" do
  
  before(:each) do
    @conn = get_connection
  end

  after(:each) do
    unless defined? JRUBY_VERSION
      @conn.logoff
    else
      @conn.close
    end
  end

  it "should connect to test database" do
    plsql.connection = @conn
    plsql.connection.raw_connection.should == @conn
  end

  it "should connect to test database using connection alias" do
    plsql(:hr).connection = @conn
    plsql(:hr).connection.raw_connection.should == @conn
  end
  
  it "should return schema name" do
    plsql.connection = @conn
    plsql.schema_name.should == DATABASE_USERS_AND_PASSWORDS[0][0].upcase
  end

  it "should return new schema name after reconnection" do
    plsql.connection = @conn
    plsql.schema_name.should == DATABASE_USERS_AND_PASSWORDS[0][0].upcase
    plsql.connection = get_connection(1)
    plsql.schema_name.should == DATABASE_USERS_AND_PASSWORDS[1][0].upcase
  end

  it "should return nil schema name if not connected" do
    plsql(:xxx).schema_name.should == nil
  end

end

describe "Named Schema" do
  before(:all) do
    plsql.connection = @conn = get_connection
  end

  after(:all) do
    plsql.connection.logoff
  end

  it "should find existing schema" do
    plsql.hr.class.should == PLSQL::Schema
  end

  it "should have the same connection as default schema" do
    plsql.hr.connection.raw_connection.should == @conn
  end

  it "should return schema name" do
    plsql.hr.schema_name.should == 'HR'
  end

  it "should not find named schema if specified twice" do
    lambda { plsql.hr.hr }.should raise_error(ArgumentError)
  end

end

describe "Schema commit and rollback" do
  before(:all) do
    plsql.connection = @conn = get_connection
  end

  after(:all) do
    plsql.connection.logoff
  end

  it "should do commit" do
    plsql.commit
  end
  
  it "should do rollback" do
    plsql.rollback
  end
end

describe "ActiveRecord connection" do
  before(:all) do
    ActiveRecord::Base.establish_connection(CONNECTION_PARAMS)
  end
  
  before(:each) do
    plsql.activerecord_class = ActiveRecord::Base
  end
  
  it "should connect to test database" do
    unless defined?(JRUBY_VERSION)
      plsql.connection.is_a?(PLSQL::OCIConnection).should be_true
    else
      plsql.connection.is_a?(PLSQL::JDBCConnection).should be_true
    end
  end
  
  it "should return schema name" do
    plsql.schema_name.should == 'HR'
  end
  
  it "should use ActiveRecord::Base.default_timezone as default" do
    ActiveRecord::Base.default_timezone = :utc
    plsql.default_timezone.should == :utc
  end
  
  it "should have the same connection as default schema" do
    plsql.hr.connection.should == plsql.connection
  end
end

describe "DBMS_OUTPUT logging" do

  before(:all) do
    plsql.connection = get_connection
    plsql.execute <<-SQL
      CREATE OR REPLACE PROCEDURE test_dbms_output(p_string VARCHAR2)
      IS
      BEGIN
        DBMS_OUTPUT.PUT_LINE(p_string);
      END;
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE PROCEDURE test_dbms_output_large(p_string VARCHAR2, p_times INTEGER)
      IS
        i INTEGER;
      BEGIN
        FOR i IN 1..p_times LOOP
          DBMS_OUTPUT.PUT_LINE(p_string);
        END LOOP;
      END;
    SQL
    @buffer = StringIO.new
  end

  before(:each) do
    @buffer.rewind
    @buffer.reopen
  end

  after(:all) do
    plsql.dbms_output_stream = nil
    plsql.execute "DROP PROCEDURE test_dbms_output"
    plsql.logoff
  end

  describe "with standard connection" do
    before(:all) do
      plsql.dbms_output_stream = @buffer
    end

    it "should log output to specified stream" do
      plsql.test_dbms_output("test_dbms_output")
      @buffer.string.should == "DBMS_OUTPUT: test_dbms_output\n"
    end

    it "should not log output to stream when output is disabled" do
      plsql.test_dbms_output("enabled")
      plsql.dbms_output_stream = nil
      plsql.test_dbms_output("disabled")
      plsql.dbms_output_stream = @buffer
      plsql.test_dbms_output("enabled again")
      @buffer.string.should == "DBMS_OUTPUT: enabled\nDBMS_OUTPUT: enabled again\n"
    end

    it "should log 20_000 character output with default buffer size" do
      times = 2_000
      plsql.test_dbms_output_large("1234567890", times)
      @buffer.string.should == "DBMS_OUTPUT: 1234567890\n" * times
    end

    it "should log 100_000 character output with specified buffer size" do
      times = 10_000
      plsql.dbms_output_buffer_size = 10 * times
      plsql.test_dbms_output_large("1234567890", times)
      @buffer.string.should == "DBMS_OUTPUT: 1234567890\n" * times
    end

  end

  describe "with Activerecord connection" do

    before(:all) do
      ActiveRecord::Base.establish_connection(CONNECTION_PARAMS)
      plsql(:ar).activerecord_class = ActiveRecord::Base
      plsql(:ar).dbms_output_stream = @buffer
    end

    it "should log output to specified stream" do
      plsql(:ar).test_dbms_output("test_dbms_output")
      @buffer.string.should == "DBMS_OUTPUT: test_dbms_output\n"
    end

    it "should log output after reconnection" do
      ActiveRecord::Base.connection.reconnect!
      plsql(:ar).test_dbms_output("after reconnection")
      @buffer.string.should == "DBMS_OUTPUT: after reconnection\n"
    end

  end

end
