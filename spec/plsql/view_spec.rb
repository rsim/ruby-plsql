require File.dirname(__FILE__) + '/../spec_helper'

describe "View" do
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.autocommit = false
    plsql.execute <<-SQL
      CREATE TABLE test_employees (
        employee_id   NUMBER(15) NOT NULL,
        first_name    VARCHAR2(50),
        last_name     VARCHAR2(50),
        hire_date     DATE,
        status        VARCHAR2(1) DEFAULT 'N'
      )
    SQL
    plsql.execute "CREATE OR REPLACE VIEW test_employees_v AS SELECT * FROM test_employees"

    @employees = (1..10).map do |i|
      {
        :employee_id => i,
        :first_name => "First #{i}",
        :last_name => "Last #{i}",
        :hire_date => Time.local(2000,01,i),
        :status => 'A'
      }
    end
    @employees_all_fields = [:employee_id, :first_name, :last_name, :hire_date, :status]
    @employees_all_values = @employees.map{|e| @employees_all_fields.map{|f| e[f]}}
    @employees_some_fields = [:employee_id, :first_name, :last_name]
    @employees_some_values = @employees.map{|e| @employees_some_fields.map{|f| e[f]}}
    @employee_default_values = {:hire_date => nil, :status => 'N'}
  end

  after(:all) do
    plsql.execute "DROP VIEW test_employees_v"
    plsql.execute "DROP TABLE test_employees"
    plsql.logoff
  end

  after(:each) do
    plsql.rollback
  end

  describe "find" do

    it "should find existing view" do
      PLSQL::View.find(plsql, :test_employees_v).should_not be_nil
    end

    it "should not find nonexisting view" do
      PLSQL::View.find(plsql, :qwerty123456).should be_nil
    end

    it "should find existing view in schema" do
      plsql.test_employees_v.should be_instance_of(PLSQL::View)
    end

  end

  describe "synonym" do

    before(:all) do
      plsql.execute "CREATE SYNONYM test_employees_v_synonym FOR hr.test_employees_v"
    end

    after(:all) do
      plsql.execute "DROP SYNONYM test_employees_v_synonym" rescue nil
    end

    it "should find synonym to view" do
      PLSQL::View.find(plsql, :test_employees_v_synonym).should_not be_nil
    end

    it "should find view using synonym in schema" do
      plsql.test_employees_v_synonym.should be_instance_of(PLSQL::View)
    end

  end

  describe "public synonym" do

    it "should find public synonym to view" do
      PLSQL::View.find(plsql, :user_tables).should_not be_nil
    end

    it "should find view using public synonym in schema" do
      plsql.user_tables.should be_instance_of(PLSQL::View)
    end

  end

  describe "columns" do

    it "should get column names for view" do
      plsql.test_employees_v.column_names.should == [:employee_id, :first_name, :last_name, :hire_date, :status]
    end

    it "should get columns metadata for view" do
      plsql.test_employees_v.columns.should == {
        :employee_id => {
          :position=>1, :data_type=>"NUMBER", :data_length=>22, :data_precision=>15, :data_scale=>0, :char_used=>nil,
          :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil, :nullable => false, :data_default => nil},
        :first_name => {
          :position=>2, :data_type=>"VARCHAR2", :data_length=>50, :data_precision=>nil, :data_scale=>nil, :char_used=>"B",
          :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil, :nullable => true, :data_default => nil},
        :last_name => {
          :position=>3, :data_type=>"VARCHAR2", :data_length=>50, :data_precision=>nil, :data_scale=>nil, :char_used=>"B",
          :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil, :nullable => true, :data_default => nil},
        :hire_date => {
          :position=>4, :data_type=>"DATE", :data_length=>7, :data_precision=>nil, :data_scale=>nil, :char_used=>nil,
          :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil, :nullable => true, :data_default => nil},
        :status => {
          :position=>5, :data_type=>"VARCHAR2", :data_length=>1, :data_precision=>nil, :data_scale=>nil, :char_used=>"B",
          :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil, :nullable => true, :data_default => nil}
      }
    end

  end

  describe "insert" do
    it "should insert a record in view" do
      plsql.test_employees_v.insert @employees.first
      plsql.test_employees_v.all.should == [@employees.first]
    end

    it "should insert a record in view using partial list of columns" do
      plsql.test_employees_v.insert @employees.first.except(:hire_date)
      plsql.test_employees_v.all.should == [@employees.first.merge(:hire_date => nil)]
    end

    it "should insert default value from table definition if value not provided" do
      plsql.test_employees_v.insert @employees.first.except(:status)
      plsql.test_employees_v.all.should == [@employees.first.merge(:status => 'N')]
    end

    it "should insert array of records in view" do
      plsql.test_employees_v.insert @employees
      plsql.test_employees_v.all("ORDER BY employee_id").should == @employees
    end

  end

  describe "insert values" do
    it "should insert a record with array of values" do
      plsql.test_employees_v.insert_values @employees_all_values.first
      plsql.test_employees_v.all.should == [@employees.first]
    end

    it "should insert a record with list of all fields and array of values" do
      plsql.test_employees_v.insert_values @employees_all_fields, @employees_all_values.first
      plsql.test_employees_v.all.should == [@employees.first]
    end

    it "should insert a record with list of some fields and array of values" do
      plsql.test_employees_v.insert_values @employees_some_fields, @employees_some_values.first
      plsql.test_employees_v.all.should == [@employees.first.merge(@employee_default_values)]
    end

    it "should insert many records with array of values" do
      plsql.test_employees_v.insert_values *@employees_all_values
      plsql.test_employees_v.all.should == @employees
    end

    it "should insert many records with list of all fields and array of values" do
      plsql.test_employees_v.insert_values @employees_all_fields, *@employees_all_values
      plsql.test_employees_v.all.should == @employees
    end

    it "should insert many records with list of some fields and array of values" do
      plsql.test_employees_v.insert_values @employees_some_fields, *@employees_some_values
      plsql.test_employees_v.all.should == @employees.map{|e| e.merge(@employee_default_values)}
    end

  end

  describe "select" do
    before(:each) do
      plsql.test_employees_v.insert @employees
    end

    it "should select first record in view" do
      plsql.test_employees_v.select(:first, "ORDER BY employee_id").should == @employees.first
      plsql.test_employees_v.first("ORDER BY employee_id").should == @employees.first
    end

    it "should select all records in view" do
      plsql.test_employees_v.select(:all, "ORDER BY employee_id").should == @employees
      plsql.test_employees_v.all("ORDER BY employee_id").should == @employees
      plsql.test_employees_v.all(:order_by => :employee_id).should == @employees
    end

    it "should select record in view using WHERE condition" do
      plsql.test_employees_v.select(:first, "WHERE employee_id = :1", @employees.first[:employee_id]).should == @employees.first
      plsql.test_employees_v.first("WHERE employee_id = :1", @employees.first[:employee_id]).should == @employees.first
      plsql.test_employees_v.first(:employee_id => @employees.first[:employee_id]).should == @employees.first
    end

    it "should select record in view using :column => nil condition" do
      employee = @employees.last
      employee[:employee_id] = employee[:employee_id] + 1
      employee[:hire_date] = nil
      plsql.test_employees_v.insert employee
      plsql.test_employees_v.first("WHERE hire_date IS NULL").should == employee
      plsql.test_employees_v.first(:hire_date => nil).should == employee
    end

    it "should count records in view" do
      plsql.test_employees_v.select(:count).should == @employees.size
      plsql.test_employees_v.count.should == @employees.size
    end

    it "should count records in view using condition" do
      plsql.test_employees_v.select(:count, "WHERE employee_id <= :1", @employees[2][:employee_id]).should == 3
      plsql.test_employees_v.count("WHERE employee_id <= :1", @employees[2][:employee_id]).should == 3
    end

  end

  describe "update" do
    it "should update a record in view" do
      employee_id = @employees.first[:employee_id]
      plsql.test_employees_v.insert @employees.first
      plsql.test_employees_v.update :first_name => 'Test', :where => {:employee_id => employee_id}
      plsql.test_employees_v.first(:employee_id => employee_id)[:first_name].should == 'Test'
    end

    it "should update a record in view using String WHERE condition" do
      employee_id = @employees.first[:employee_id]
      plsql.test_employees_v.insert @employees
      plsql.test_employees_v.update :first_name => 'Test', :where => "employee_id = #{employee_id}"
      plsql.test_employees_v.first(:employee_id => employee_id)[:first_name].should == 'Test'
      # all other records should not be changed
      plsql.test_employees_v.all("WHERE employee_id > :1", employee_id) do |employee|
        employee[:first_name].should_not == 'Test'
      end
    end

    it "should update all records in view" do
      plsql.test_employees_v.insert @employees
      plsql.test_employees_v.update :first_name => 'Test'
      plsql.test_employees_v.all do |employee|
        employee[:first_name].should == 'Test'
      end
    end

  end

  describe "delete" do
    it "should delete record from view" do
      employee_id = @employees.first[:employee_id]
      plsql.test_employees_v.insert @employees
      plsql.test_employees_v.delete :employee_id => employee_id
      plsql.test_employees_v.first(:employee_id => employee_id).should be_nil
      plsql.test_employees_v.all(:order_by => :employee_id).should == @employees[1, @employees.size-1]
    end

    it "should delete all records from view" do
      plsql.test_employees_v.insert @employees
      plsql.test_employees_v.delete
      plsql.test_employees_v.all.should be_empty
    end
  end

end
