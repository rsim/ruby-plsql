require File.dirname(__FILE__) + '/../spec_helper'

require "rubygems"
require "activerecord"

describe "Function with string parameters" do
  
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE FUNCTION test_uppercase
        ( p_string VARCHAR2 )
        RETURN VARCHAR2
      IS
      BEGIN
        RETURN UPPER(p_string);
      END test_uppercase;
    EOS
  end
  
  after(:all) do
    plsql.logoff
  end
  
  it "should find existing procedure" do
    PLSQL::Procedure.find(plsql, :test_uppercase).should_not be_nil
  end

  it "should not find nonexisting procedure" do
    PLSQL::Procedure.find(plsql, :qwerty123456).should be_nil
  end

  it "should execute function and return correct value" do
    plsql.test_uppercase('xxx').should == 'XXX'
  end

  it "should execute function with named parameters and return correct value" do
    plsql.test_uppercase(:p_string => 'xxx').should == 'XXX'
  end

  it "should raise error if wrong number of arguments is passed" do
    lambda { plsql.test_uppercase('xxx','yyy') }.should raise_error(ArgumentError)
  end

  it "should raise error if wrong named argument is passed" do
    lambda { plsql.test_uppercase(:p_string2 => 'xxx') }.should raise_error(ArgumentError)
  end
  
  it "should execute function with schema name specified" do
    plsql.hr.test_uppercase('xxx').should == 'XXX'
  end

  it "should process nil parameter as NULL" do
    plsql.test_uppercase(nil).should be_nil
  end

end

describe "Function with numeric parameters" do
  
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE FUNCTION test_sum
        ( p_num1 NUMBER, p_num2 NUMBER )
        RETURN NUMBER
      IS
      BEGIN
        RETURN p_num1 + p_num2;
      END test_sum;
    EOS
  end
  
  after(:all) do
    plsql.logoff
  end
  
  it "should process integer parameters" do
    plsql.test_sum(123,456).should == 579
  end

  it "should process big integer parameters" do
    plsql.test_sum(123123123123,456456456456).should == 579579579579
  end

  it "should process float parameters" do
    plsql.test_sum(123.123,456.456).should == 579.579
  end

  it "should process BigDecimal parameters" do
    plsql.test_sum(:p_num1 => BigDecimal.new("123.123"), :p_num2 => BigDecimal.new("456.456")).should == 579.579
  end

  it "should process nil parameter as NULL" do
    plsql.test_sum(123,nil).should be_nil
  end

end

describe "Function with date parameters" do
  
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE FUNCTION test_date
        ( p_date DATE )
        RETURN DATE
      IS
      BEGIN
        RETURN p_date + 1;
      END test_date;
    EOS
  end
  
  after(:all) do
    plsql.logoff
  end
  
  it "should process Time parameters" do
    now = Time.local(2008,8,12,14,28,0)
    plsql.test_date(now).should == now + 60*60*24
  end

  it "should process DateTime parameters" do
    now = DateTime.parse(Time.local(2008,8,12,14,28,0).iso8601)
    result = plsql.test_date(now)
    result.class.should == Time
    result.should == Time.parse((now + 1).strftime("%c"))
  end
  
  it "should process old DateTime parameters" do
    now = DateTime.new(1901,1,1,12,0,0)
    result = plsql.test_date(now)
    unless defined?(JRUBY_VERSION)
      result.class.should == DateTime
      result.should == now + 1
    else
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
    end
  end

  it "should process Date parameters" do
    now = Date.new(2008,8,12)
    result = plsql.test_date(now)
    result.class.should == Time    
    result.should == Time.parse((now + 1).strftime("%c"))
  end
  
  it "should process old Date parameters" do
    now = Date.new(1901,1,1)
    result = plsql.test_date(now)
    unless defined?(JRUBY_VERSION)
      # result.class.should == DateTime
      result.should == now + 1
    else
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
    end
  end
  
  it "should process nil date parameter as NULL" do
    plsql.test_date(nil).should be_nil
  end

end

describe "Function with timestamp parameters" do
  
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE FUNCTION test_timestamp
        ( p_time TIMESTAMP )
        RETURN TIMESTAMP
      IS
      BEGIN
        RETURN p_time + 1;
      END test_timestamp;
    EOS
  end
  
  after(:all) do
    plsql.logoff
  end
  
  it "should process timestamp parameters" do
    now = Time.local(2008,8,12,14,28,0)
    plsql.test_timestamp(now).should == now + 60*60*24
  end

end

describe "Procedure with output parameters" do
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE PROCEDURE test_copy
        ( p_from VARCHAR2, p_to OUT VARCHAR2, p_to_double OUT VARCHAR2 )
      IS
      BEGIN
        p_to := p_from;
        p_to_double := p_from || p_from;
      END test_copy;
    EOS
  end
  
  after(:all) do
    plsql.logoff
  end
  
  it "should return hash with output parameters" do
    plsql.test_copy("abc", nil, nil).should == { :p_to => "abc", :p_to_double => "abcabc" }
  end

  it "should return hash with output parameters when called with named parameters" do
    plsql.test_copy(:p_from => "abc", :p_to => nil, :p_to_double => nil).should == { :p_to => "abc", :p_to_double => "abcabc" }
  end

  it "should substitute output parameters with nil if they are not specified" do
    plsql.test_copy("abc").should == { :p_to => "abc", :p_to_double => "abcabc" }
  end

  it "should substitute all parementers with nil if none are specified" do
    plsql.test_copy.should == { :p_to => nil, :p_to_double => nil }
  end

end

describe "Package with procedures with same name but different argument lists" do
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE PACKAGE test_package2 IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2;
        PROCEDURE test_procedure ( p_string VARCHAR2, p_result OUT VARCHAR2 )
          ;
        PROCEDURE test_procedure ( p_number NUMBER, p_result OUT VARCHAR2 )
          ;
        FUNCTION test_procedure2 ( p_string VARCHAR2 )
          RETURN VARCHAR2;
      END;
    EOS
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE PACKAGE BODY test_package2 IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_procedure;
        PROCEDURE test_procedure ( p_string VARCHAR2, p_result OUT VARCHAR2 )
        IS
        BEGIN
          p_result := UPPER(p_string);
        END test_procedure;
        PROCEDURE test_procedure ( p_number NUMBER, p_result OUT VARCHAR2 )
        IS
        BEGIN
          p_result := LOWER(TO_CHAR(p_number));
        END test_procedure;
        FUNCTION test_procedure2 ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_procedure2;
      END;
    EOS

  end
  
  after(:all) do
    plsql.logoff
  end
    
  it "should find existing package" do
    PLSQL::Package.find(plsql, :test_package2).should_not be_nil
  end

  it "should identify overloaded procedure definition" do
    @procedure = PLSQL::Procedure.find(plsql, :test_procedure, "TEST_PACKAGE2")
    @procedure.should_not be_nil
    @procedure.should be_overloaded
  end

  it "should identify non-overloaded procedure definition" do
    @procedure = PLSQL::Procedure.find(plsql, :test_procedure2, "TEST_PACKAGE2")
    @procedure.should_not be_nil
    @procedure.should_not be_overloaded
  end

  it "should execute correct procedures based on number of arguments and return correct value" do
    plsql.test_package2.test_procedure('xxx').should == 'XXX'
    plsql.test_package2.test_procedure('xxx', nil).should == {:p_result => 'XXX'}
  end

  it "should execute correct procedures based on number of named arguments and return correct value" do
    plsql.test_package2.test_procedure(:p_string => 'xxx').should == 'XXX'
    plsql.test_package2.test_procedure(:p_string => 'xxx', :p_result => nil).should == {:p_result => 'XXX'}
  end

  it "should raise exception if procedure cannot be found based on number of arguments" do
    lambda { plsql.test_package2.test_procedure() }.should raise_error(ArgumentError)
  end
  
  # TODO: should try to implement matching by types of arguments
  # it "should find procedure based on types of arguments" do
  #   plsql.test_package2.test_procedure(111, nil).should == {:p_result => '111'}
  # end

  it "should find procedure based on names of named arguments" do
    plsql.test_package2.test_procedure(:p_number => 111, :p_result => nil).should == {:p_result => '111'}
  end

end

describe "Function with output parameters" do
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE FUNCTION test_copy_function
        ( p_from VARCHAR2, p_to OUT VARCHAR2, p_to_double OUT VARCHAR2 )
        RETURN NUMBER
      IS
      BEGIN
        p_to := p_from;
        p_to_double := p_from || p_from;
        RETURN LENGTH(p_from);
      END test_copy_function;
    EOS
  end
  
  after(:all) do
    plsql.logoff
  end
  
  it "should return array with return value and hash of output parameters" do
    plsql.test_copy_function("abc", nil, nil).should == [3, { :p_to => "abc", :p_to_double => "abcabc" }]
  end

  it "should return array with return value and hash of output parameters when called with named parameters" do
    plsql.test_copy_function(:p_from => "abc", :p_to => nil, :p_to_double => nil).should == 
      [3, { :p_to => "abc", :p_to_double => "abcabc" }]
  end

  it "should substitute output parameters with nil if they are not specified" do
    plsql.test_copy_function("abc").should == [3, { :p_to => "abc", :p_to_double => "abcabc" }]
  end

  it "should substitute all parementers with nil if none are specified" do
    plsql.test_copy_function.should == [nil, { :p_to => nil, :p_to_double => nil }]
  end

end

describe "Function without parameters" do
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE FUNCTION test_no_params
        RETURN VARCHAR2
      IS
      BEGIN
        RETURN 'dummy';
      END test_no_params;
    EOS
  end
  
  after(:all) do
    plsql.logoff
  end

  it "should return value" do
    plsql.test_no_params.should == "dummy"
  end
end