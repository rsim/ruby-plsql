require 'spec_helper'

describe "Schema" do

  it "should create Schema object" do
    expect(plsql.class).to eq(PLSQL::Schema)
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
    expect(plsql.connection.raw_connection).to eq(@conn)
  end

  it "should connect to test database using connection alias" do
    plsql(:hr).connection = @conn
    expect(plsql(:hr).connection.raw_connection).to eq(@conn)
  end

  it "should return schema name" do
    plsql.connection = @conn
    expect(plsql.schema_name).to eq(DATABASE_USERS_AND_PASSWORDS[0][0].upcase)
  end

  it 'should match altered current_schema in database session' do
    plsql.connection = @conn
    expected_current_schema = DATABASE_USERS_AND_PASSWORDS[1][0]
    plsql.execute "ALTER SESSION set current_schema=#{expected_current_schema}"
    expect(plsql.schema_name).to eq(expected_current_schema.upcase)
  end

  it "should return new schema name after reconnection" do
    plsql.connection = @conn
    expect(plsql.schema_name).to eq(DATABASE_USERS_AND_PASSWORDS[0][0].upcase)
    plsql.connection = get_connection(1)
    expect(plsql.schema_name).to eq(DATABASE_USERS_AND_PASSWORDS[1][0].upcase)
  end

  it "should return nil schema name if not connected" do
    expect(plsql(:xxx).schema_name).to eq(nil)
  end

end

describe "Connection with connect!" do

  before(:all) do
    @username, @password = DATABASE_USERS_AND_PASSWORDS[0]
    @database = DATABASE_NAME
    @database_service = DATABASE_SERVICE_NAME
    @host = DATABASE_HOST
    @port = DATABASE_PORT
  end

  after(:each) do
    plsql.logoff if plsql.connection
  end

  it "should connect with username, password and database alias" do
    plsql.connect! @username, @password, @database
    expect(plsql.connection).not_to be_nil
    expect(plsql.schema_name).to eq(@username.upcase)
  end

  it "should connect with username, password, host, port and database name" do
    plsql.connect! @username, @password, :host => @host, :port => @port, :database => @database_service
    expect(plsql.connection).not_to be_nil
    expect(plsql.schema_name).to eq(@username.upcase)
  end

  it "should connect with username, password, host, database name and default port" do
    skip "Non-default port used for test database" unless @port == 1521
    plsql.connect! @username, @password, :host => @host, :database => @database_service
    expect(plsql.connection).not_to be_nil
    expect(plsql.schema_name).to eq(@username.upcase)
  end

  it "should not connect with wrong port number" do
    expect {
      plsql.connect! @username, @password, :host => @host, :port => 9999, :database => @database
    }.to raise_error(/ORA-12541|could not establish the connection/)
  end

  it "should connect with one Hash parameter" do
    plsql.connect! :username => @username, :password => @password, :database => @database
    expect(plsql.connection).not_to be_nil
    expect(plsql.schema_name).to eq(@username.upcase)
  end

  it "should set session time zone from ORA_SDTZ environment variable" do
    plsql.connect! @username, @password, @database
    expect(plsql.connection.time_zone).to eq(ENV['ORA_SDTZ'])
  end if ENV['ORA_SDTZ']


  it "should set session time zone from :time_zone parameter" do
    plsql.connect! :username => @username, :password => @password, :database => @database, :time_zone => 'EET'
    expect(plsql.connection.time_zone).to eq('EET')
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
    expect(plsql.hr.class).to eq(PLSQL::Schema)
  end

  it "should have the same connection as default schema" do
    expect(plsql.hr.connection.raw_connection).to eq(@conn)
  end

  it "should return schema name" do
    expect(plsql.hr.schema_name).to eq('HR')
  end

  it "should not find named schema if specified twice" do
    expect { plsql.hr.hr }.to raise_error(ArgumentError)
  end

end

describe "Schema commit and rollback" do
  before(:all) do
    plsql.connection = @conn = get_connection
    plsql.connection.autocommit = false
    plsql.execute "CREATE TABLE test_commit (dummy VARCHAR2(100))"
    @data = {:dummy => 'test'}
    @data2 = {:dummy => 'test2'}
  end

  after(:all) do
    plsql.execute "DROP TABLE test_commit"
    plsql.logoff
  end

  after(:each) do
    plsql.test_commit.delete
    plsql.commit
  end

  it "should do commit" do
    plsql.test_commit.insert @data
    plsql.commit
    expect(plsql.test_commit.first).to eq(@data)
  end

  it "should do rollback" do
    plsql.test_commit.insert @data
    plsql.rollback
    expect(plsql.test_commit.first).to be_nil
  end

  it "should create savepoint and rollback to savepoint" do
    plsql.test_commit.insert @data
    plsql.savepoint 'test'
    plsql.test_commit.insert @data2
    expect(plsql.test_commit.all).to eq([@data, @data2])
    plsql.rollback_to 'test'
    expect(plsql.test_commit.all).to eq([@data])
  end

end

describe "ActiveRecord connection" do
  before(:all) do
    ActiveRecord::Base.establish_connection(CONNECTION_PARAMS)
    class TestBaseModel < ActiveRecord::Base
      self.abstract_class = true
    end
    class TestModel < TestBaseModel
    end
  end

  before(:each) do
    plsql.activerecord_class = ActiveRecord::Base
  end

  it "should connect to test database" do
    unless defined?(JRUBY_VERSION)
      expect(plsql.connection.is_a?(PLSQL::OCIConnection)).to be_truthy
    else
      expect(plsql.connection.is_a?(PLSQL::JDBCConnection)).to be_truthy
    end
  end

  it "should return schema name" do
    expect(plsql.schema_name).to eq('HR')
  end

  it "should use ActiveRecord::Base.default_timezone as default" do
    ActiveRecord::Base.default_timezone = :utc
    expect(plsql.default_timezone).to eq(:utc)
  end

  it "should have the same connection as default schema" do
    expect(plsql.hr.connection).to eq(plsql.connection)
  end

  it "should accept inherited ActiveRecord class" do
    plsql.activerecord_class = TestBaseModel
    expect(plsql.schema_name).to eq('HR')
  end

  it "should accept subclass of inherited ActiveRecord class" do
    plsql.activerecord_class = TestModel
    expect(plsql.schema_name).to eq('HR')
  end

  it "should safely close cursors in threaded environment" do
    expect {
      t1 = Thread.new { plsql.dbms_lock.sleep(1) }.tap { |t| t.abort_on_exception = true }
      t2 = Thread.new { plsql.dbms_lock.sleep(2) }.tap { |t| t.abort_on_exception = true }
      [t2, t1].each { |t| t.join }
    }.not_to raise_error
  end

end if defined?(ActiveRecord)

describe "DBMS_OUTPUT logging" do

  before(:all) do
    plsql.connection = get_connection
    plsql.execute <<-SQL
      CREATE OR REPLACE PROCEDURE test_dbms_output(p_string VARCHAR2, p_raise_error BOOLEAN := false)
      IS
      BEGIN
        DBMS_OUTPUT.PUT_LINE(p_string);
        IF p_raise_error THEN
          RAISE_APPLICATION_ERROR(-20000 - 12, 'Test Error');
        END IF;
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
    plsql.execute "DROP PROCEDURE test_dbms_output_large"
    plsql.logoff
  end

  describe "with standard connection" do
    before(:all) do
      plsql.dbms_output_stream = @buffer
    end

    before(:each) do
      plsql.dbms_output_buffer_size = nil
    end

    it "should log output to specified stream" do
      plsql.test_dbms_output("test_dbms_output")
      expect(@buffer.string).to eq("DBMS_OUTPUT: test_dbms_output\n")
    end

    it "should log output to specified stream in case of exception" do
      expect { plsql.test_dbms_output("test_dbms_output", true) }.to raise_error /Test Error/
      expect(@buffer.string).to eq("DBMS_OUTPUT: test_dbms_output\n")
    end

    it "should not log output to stream when output is disabled" do
      plsql.test_dbms_output("enabled")
      plsql.dbms_output_stream = nil
      plsql.test_dbms_output("disabled")
      plsql.dbms_output_stream = @buffer
      plsql.test_dbms_output("enabled again")
      expect(@buffer.string).to eq("DBMS_OUTPUT: enabled\nDBMS_OUTPUT: enabled again\n")
    end

    it "should log 20_000 character output with default buffer size" do
      times = 2_000
      plsql.test_dbms_output_large("1234567890", times)
      expect(@buffer.string).to eq("DBMS_OUTPUT: 1234567890\n" * times)
    end

    it "should log 100_000 character output with specified buffer size" do
      times = 10_000
      plsql.dbms_output_buffer_size = 10 * times
      plsql.test_dbms_output_large("1234567890", times)
      expect(@buffer.string).to eq("DBMS_OUTPUT: 1234567890\n" * times)
    end

    it "should log output when database version is less than 10.2" do
      allow(plsql.connection).to receive(:database_version).and_return([9, 2, 0, 0])
      times = 2_000
      plsql.test_dbms_output_large("1234567890", times)
      expect(@buffer.string).to eq("DBMS_OUTPUT: 1234567890\n" * times)
    end

    it "should log output when calling procedure with schema prefix" do
      plsql.hr.test_dbms_output("test_dbms_output")
      expect(@buffer.string).to eq("DBMS_OUTPUT: test_dbms_output\n")
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
      expect(@buffer.string).to eq("DBMS_OUTPUT: test_dbms_output\n")
    end

    it "should log output after reconnection" do
      ActiveRecord::Base.connection.reconnect!
      plsql(:ar).test_dbms_output("after reconnection")
      expect(@buffer.string).to eq("DBMS_OUTPUT: after reconnection\n")
    end

  end if defined?(ActiveRecord)

end
