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
    plsql.schema_name.should == 'HR'
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
  
  it "should user ActiveRecord::Base.default_timezone as default" do
    ActiveRecord::Base.default_timezone = :utc
    plsql.default_timezone.should == :utc
  end
  
end