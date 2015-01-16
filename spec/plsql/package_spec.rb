require 'spec_helper'

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
    expect(PLSQL::Package.find(plsql, :test_package)).not_to be_nil
  end

  it "should not find nonexisting package" do
    expect(PLSQL::Package.find(plsql, :qwerty123456)).to be_nil
  end

  it "should find existing package in schema" do
    expect(plsql.test_package.class).to eq(PLSQL::Package)
  end

  it "should execute package function and return correct value" do
    expect(plsql.test_package.test_procedure('xxx')).to eq('XXX')
  end

  it "should report an existing procedure as existing" do
    expect(plsql.test_package.procedure_defined?(:test_procedure)).to be_truthy
  end

  it "should report an inexistent procedure as not existing" do
    expect(plsql.test_package.procedure_defined?(:inexistent_procedure)).to be_falsey
  end

  describe "variables" do
    it "should set and get package variable value" do
      plsql.test_package.test_variable = 1
      expect(plsql.test_package.test_variable).to eq(1)
    end
  end

end

describe "Synonym to package" do
  
  before(:all) do
    plsql.connection = get_connection
    plsql.execute <<-SQL
      CREATE OR REPLACE PACKAGE hr.test_package IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2;
      END;
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE PACKAGE BODY hr.test_package IS
        FUNCTION test_procedure ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_procedure;
      END;
    SQL
    plsql.execute "CREATE SYNONYM test_pkg_synonym FOR hr.test_package"
  end
  
  after(:all) do
    plsql.execute "DROP SYNONYM test_pkg_synonym" rescue nil
    plsql.logoff
  end
  
  it "should find synonym to package" do
    expect(PLSQL::Package.find(plsql, :test_pkg_synonym)).not_to be_nil
  end

  it "should execute package function using synonym and return correct value" do
    expect(plsql.test_pkg_synonym.test_procedure('xxx')).to eq('XXX')
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
    expect(PLSQL::Package.find(plsql, :utl_encode)).not_to be_nil
  end

  it "should execute package function using public synonym and return correct value" do
    expect(plsql.utl_encode.base64_encode('abc')).to eq('4372773D')
  end

end
