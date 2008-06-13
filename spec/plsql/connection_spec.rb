require File.dirname(__FILE__) + '/../spec_helper'

describe "Connection" do

  unless defined?(JRUBY_VERSION)

    before(:all) do
      @raw_conn = OCI8.new("hr","hr","xe")    
    end

  else

    before(:all) do
      @raw_conn = DriverManager.getConnection("jdbc:oracle:thin:@ubuntu710:1521:XE","hr","hr")
    end

  end

  before(:each) do
    @conn = PLSQL::Connection.create( @raw_conn )
  end

  it "should create connection" do
    @conn.raw_connection.should == @raw_conn
  end

  unless defined?(JRUBY_VERSION)
    it "should be oci connection" do
      @conn.should be_oci
      @conn.raw_driver.should == :oci
    end
  else
    it "should be jdbc connection" do
      @conn.should be_jdbc
      @conn.raw_driver.should == :jdbc
    end
  end
  
  it "should execute SQL statement and return first result" do
    @now = Time.local(2008,05,31,23,22,11)
    @conn.select_first("SELECT 'abc',123,123.456,
      TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
      FROM dual").should == ["abc",123,123.456,@now]
  end


  # it "should execute SQL statement" do
  #   @conn.exec("SELECT * FROM dual").should == @conn.raw_connection.exec("SELECT * FROM dual")
  # end

end
