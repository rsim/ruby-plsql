require File.dirname(__FILE__) + '/../spec_helper'

describe "Schema" do
  
  it "should create Schema object" do
    plsql.class.should == PLSQL::Schema
  end
  
end

describe "Connection" do
  
  before(:each) do
    @conn = OCI8.new("hr","hr","xe")
  end

  after(:each) do
    @conn.logoff
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
    @conn = OCI8.new("hr","hr","xe")
    plsql.connection = @conn
  end

  after(:all) do
    @conn.logoff
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