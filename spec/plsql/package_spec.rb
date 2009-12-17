require File.dirname(__FILE__) + '/../spec_helper'

describe "Package" do
  before(:all) do
    plsql.connection = get_connection
    plsql.execute <<-SQL
      CREATE OR REPLACE PACKAGE test_package IS
        test_variable NUMBER;
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2;
      END;
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE PACKAGE BODY test_package IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_procedure;
      END;
    SQL

  end
  
  after(:all) do
    plsql.execute "DROP PACKAGE test_package"
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

  describe "variables" do
    it "should set and get package variable value" do
      plsql.test_package.test_variable = 1
      plsql.test_package.test_variable.should == 1
    end
  end

end

describe "Synonym to package" do
  
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE PACKAGE hr.test_package IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2;
      END;
    EOS
    plsql.connection.exec <<-EOS
      CREATE OR REPLACE PACKAGE BODY hr.test_package IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_procedure;
      END;
    EOS
    plsql.connection.exec "CREATE SYNONYM test_pkg_synonym FOR hr.test_package"
  end
  
  after(:all) do
    plsql.connection.exec "DROP SYNONYM test_pkg_synonym" rescue nil
    plsql.logoff
  end
  
  it "should find synonym to package" do
    PLSQL::Package.find(plsql, :test_pkg_synonym).should_not be_nil
  end

  it "should execute package function using synonym and return correct value" do
    plsql.test_pkg_synonym.test_procedure('xxx').should == 'XXX'
  end

end

describe "Public synonym to package" do
  
  before(:all) do
    plsql.connection = get_connection
  end
  
  after(:all) do
    plsql.logoff
  end
  
  it "should find public synonym to package" do
    PLSQL::Package.find(plsql, :utl_encode).should_not be_nil
  end

  it "should execute package function using public synonym and return correct value" do
    plsql.utl_encode.base64_encode('abc').should == '4372773D'
  end

end
