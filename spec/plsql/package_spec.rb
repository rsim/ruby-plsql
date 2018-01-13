require "spec_helper"

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
    expect(plsql.test_package.test_procedure("xxx")).to eq("XXX")
  end

  it "should report an existing procedure as existing" do
    expect(plsql.test_package.procedure_defined?(:test_procedure)).to be_truthy
  end

  it "should report an inexistent procedure as not existing" do
    expect(plsql.test_package.procedure_defined?(:inexistent_procedure)).to be_falsey
  end

  it "should search objects via []" do
    package = PLSQL::Package.find(plsql, :test_package)

    [:Test_Procedure, :test_procedure, "test_procedure", "TEST_PROCEDURE"].each do |name_variant|
      expect(package[name_variant]).to be_a PLSQL::Procedure
    end

    [:Test_Variable, :test_variable, "test_variable", "TEST_VARIABLE"].each do |name_variant|
      expect(package[name_variant]).to be_a PLSQL::Variable
    end
  end

  context "with a user with execute privilege who is not the package owner" do
    before(:all) do
      plsql.execute("grant execute on TEST_PACKAGE to #{DATABASE_USERS_AND_PASSWORDS[1][0]}")
      @original_connection = plsql.connection
      @conn = get_connection(1)
    end

    before(:each) do
      # resetting connection clears cached package objects and schema name
      plsql.connection = @conn
    end

    after(:all) do
      plsql.logoff
      plsql.connection = @original_connection
    end

    it "should not find existing package" do
      expect(PLSQL::Package.find(plsql, :test_package)).to be_nil
    end

    context "who sets current_schema to match the package owner" do
      before(:all) do
        plsql.execute "ALTER SESSION set current_schema=#{DATABASE_USERS_AND_PASSWORDS[0][0]}"
      end

      it "should find existing package" do
        expect(PLSQL::Package.find(plsql, :test_package)).not_to be_nil
      end

      it "should report an existing procedure as existing" do
        expect(plsql.test_package.procedure_defined?(:test_procedure)).to be_truthy
      end

    end

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
    expect(plsql.test_pkg_synonym.test_procedure("xxx")).to eq("XXX")
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
    expect(plsql.utl_encode.base64_encode("abc")).to eq("4372773D")
  end

end
