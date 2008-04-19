require File.dirname(__FILE__) + '/../spec_helper'

describe "Package" do
  before(:all) do
    plsql.connection = conn = OCI8.new("hr","hr","xe")
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE PACKAGE test_package IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2;
      END;
    EOS
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE PACKAGE BODY test_package IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_procedure;
      END;
    EOS

  end
  
  after(:all) do
    plsql.logoff
  end
  
  before(:each) do
  end
  
  it "should find existing package" do
    PLSQL::Package.find(plsql, :test_package).should_not be_nil
  end

  it "should not find nonexisting package" do
    PLSQL::Package.find(plsql, :qwerty123456).should be_nil
  end

  it "should find existing package in schema" do
    plsql.test_package.class.should == PLSQL::Package
  end

  it "should execute package function and return correct value" do
    plsql.test_package.test_procedure('xxx').should == 'XXX'
  end

end
