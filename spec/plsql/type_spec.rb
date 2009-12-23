require File.dirname(__FILE__) + '/../spec_helper'

describe "Type" do
  before(:all) do
    plsql.connection = get_connection
    plsql.execute "DROP TYPE t_employee" rescue nil
    plsql.execute "DROP TYPE t_phones" rescue nil
    plsql.connection.exec <<-SQL
      CREATE OR REPLACE TYPE t_address AS OBJECT (
        street    VARCHAR2(50),
        city      VARCHAR2(50),
        country   VARCHAR2(50)
      )
    SQL
    plsql.connection.exec <<-SQL
      CREATE OR REPLACE TYPE t_phone AS OBJECT (
        type            VARCHAR2(10),
        phone_number    VARCHAR2(50)
      )
    SQL
    plsql.connection.exec <<-SQL
      CREATE OR REPLACE TYPE t_phones AS VARRAY(10) OF T_PHONE
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE TYPE t_employee AS OBJECT (
        employee_id   NUMBER(15),
        first_name    VARCHAR2(50),
        last_name     VARCHAR2(50),
        hire_date     DATE,
        address       t_address,
        phones        t_phones
      )
    SQL
  end

  after(:all) do
    plsql.execute "DROP TYPE t_employee"
    plsql.execute "DROP TYPE t_address"
    plsql.execute "DROP TYPE t_phones"
    plsql.execute "DROP TYPE t_phone"
    plsql.logoff
  end

  after(:each) do
    plsql.rollback
  end

  describe "find" do

    it "should find existing type" do
      PLSQL::Type.find(plsql, :t_employee).should_not be_nil
    end

    it "should not find nonexisting type" do
      PLSQL::Type.find(plsql, :qwerty123456).should be_nil
    end

    it "should find existing type in schema" do
      plsql.t_employee.should be_a(PLSQL::Type)
    end

  end

  describe "synonym" do

    before(:all) do
      plsql.connection.exec "CREATE SYNONYM t_employee_synonym FOR hr.t_employee"
    end

    after(:all) do
      plsql.connection.exec "DROP SYNONYM t_employee_synonym" rescue nil
    end

    it "should find synonym to type" do
      PLSQL::Type.find(plsql, :t_employee_synonym).should_not be_nil
    end

    it "should find type using synonym in schema" do
      plsql.t_employee_synonym.should be_a(PLSQL::Type)
    end

  end

  describe "public synonym" do

    it "should find public synonym to type" do
      PLSQL::Type.find(plsql, :xmltype).should_not be_nil
    end

    it "should find type using public synonym in schema" do
      plsql.xmltype.should be_a(PLSQL::Type)
    end

  end

  describe "typecode" do

    it "should get typecode of object type" do
      plsql.t_employee.typecode.should == "OBJECT"
    end

    it "should get typecode of collection type" do
      plsql.t_phones.typecode.should == "COLLECTION"
    end

  end

  describe "attributes" do

    it "should get attribute names" do
      plsql.t_employee.attribute_names.should == [:employee_id, :first_name, :last_name, :hire_date, :address, :phones]
    end

    it "should get attributes metadata" do
      plsql.t_employee.attributes.should == {
        :employee_id =>
          {:position=>1, :data_type=>"NUMBER", :data_length=>nil, :data_precision=>15, :data_scale=>0, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :first_name =>
          {:position=>2, :data_type=>"VARCHAR2", :data_length=>50, :data_precision=>nil, :data_scale=>nil, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :last_name =>
          {:position=>3, :data_type=>"VARCHAR2", :data_length=>50, :data_precision=>nil, :data_scale=>nil, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :hire_date => 
          {:position=>4, :data_type=>"DATE", :data_length=>nil, :data_precision=>nil, :data_scale=>nil, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :address => 
          {:position=>5, :data_type=>"OBJECT", :data_length=>nil, :data_precision=>nil, :data_scale=>nil, :type_owner=>"HR", :type_name=>"T_ADDRESS", :sql_type_name=>"HR.T_ADDRESS"},
        :phones => 
          {:position=>6, :data_type=>"TABLE", :data_length=>nil, :data_precision=>nil, :data_scale=>nil, :type_owner=>"HR", :type_name=>"T_PHONES", :sql_type_name=>"HR.T_PHONES"}
      }
    end

  end

end
