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
  
  it "should logoff connection" do
    @conn.logoff.should be_true
  end
  
  describe "SQL statements" do

    it "should execute SQL statement and return first result" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_first("SELECT 'abc',123,123.456,
        TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
        FROM dual").should == ["abc",123,123.456,@now]
    end

    it "should execute SQL statement with bind parameters and return first result" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_first("SELECT :1,:2,:3,:4 FROM dual",
        'abc',123,123.456,@now).should == ["abc",123,123.456,@now]
    end

    it "should execute SQL statement and return all results" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual
          UNION ALL SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual").should == [["abc",123,123.456,@now],["abc",123,123.456,@now]]
    end

    it "should execute SQL statement with bind parameters and return all results" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT :1,:2,:3,:4 FROM dual UNION ALL SELECT :1,:2,:3,:4 FROM dual",
        'abc',123,123.456,@now,'abc',123,123.456,@now).should == [["abc",123,123.456,@now],["abc",123,123.456,@now]]
    end

    it "should execute SQL statement and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual
          UNION ALL SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual") do |r|
        r.should == ["abc",123,123.456,@now]
      end.should == 2
    end

    it "should execute SQL statement with bind parameters and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT :1,:2,:3,:4 FROM dual UNION ALL SELECT :1,:2,:3,:4 FROM dual",
        'abc',123,123.456,@now,'abc',123,123.456,@now) do |r|
        r.should == ["abc",123,123.456,@now]
      end.should == 2
    end

  end
  
  describe "PL/SQL procedures" do
    before(:each) do
      @random = rand(1000)
      sql = <<-EOS
        CREATE OR REPLACE FUNCTION test_add_random (p_number NUMBER)
          RETURN NUMBER
        IS
        BEGIN
          RETURN p_number + #{@random};
        END test_add_random;
      EOS
      @conn.exec(sql).should be_true
    end

    it "should execute PL/SQL procedure definition" do
      @conn.select_first("SELECT test_add_random(1) FROM dual").should == [@random + 1]
    end

    it "should parse PL/SQL procedure call and bind parameters and exec and get bind parameter value" do
      sql = <<-EOS
        BEGIN
          :result := test_add_random (:p_number);
        END;
      EOS
      cursor = @conn.parse(sql)
      cursor.bind_param(":result",nil,Fixnum)
      cursor.bind_param(":p_number",100,Fixnum,3)
      cursor.exec
      cursor[":result"].should == @random + 100
      cursor.close.should be_nil
    end

  end

end
