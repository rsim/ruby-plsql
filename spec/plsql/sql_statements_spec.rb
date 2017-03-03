require 'spec_helper'

describe "SQL statements /" do
  before(:all) do
    plsql.connect! CONNECTION_PARAMS
    plsql.connection.autocommit = false
  end

  after(:all) do
    plsql.logoff
  end

  after(:each) do
    plsql.rollback
  end

  describe "SELECT" do
    before(:all) do
      plsql.execute "DROP TABLE test_employees" rescue nil
      plsql.execute <<-SQL
        CREATE TABLE test_employees (
          employee_id   NUMBER(15),
          first_name    VARCHAR2(50),
          last_name     VARCHAR(50),
          hire_date     DATE
        )
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_insert_employee(p_employee test_employees%ROWTYPE)
        IS
        BEGIN
          INSERT INTO test_employees
          VALUES p_employee;
        END;
      SQL
      @employees = (1..10).map do |i|
        {
          :employee_id => i,
          :first_name => "First #{i}",
          :last_name => "Last #{i}",
          :hire_date => Time.local(2000,01,i)
        }
      end
      plsql.connection.prefetch_rows = 100
    end

    before(:each) do
      @employees.each do |e|
        plsql.test_insert_employee(e)
      end
    end

    after(:all) do
      plsql.execute "DROP PROCEDURE test_insert_employee"
      plsql.execute "DROP TABLE test_employees"
      plsql.connection.prefetch_rows = 1
    end

    it "should select first result" do
      expect(plsql.select(:first, "SELECT * FROM test_employees WHERE employee_id = :employee_id",
        @employees.first[:employee_id])).to eq(@employees.first)
    end

    it "should prefetch only one row when selecting first result" do
      expect {
        plsql.select(:first, "SELECT 1 FROM dual UNION ALL SELECT 1/0 FROM dual")
      }.not_to raise_error
    end

    it "should select one value" do
      expect(plsql.select_one("SELECT count(*) FROM test_employees")).to eq(@employees.size)
    end

    it "should return nil when selecting non-existing one value" do
      expect(plsql.select_one("SELECT employee_id FROM test_employees WHERE 1=2")).to be_nil
    end

    it "should prefetch only one row when selecting one value" do
      expect {
        plsql.select_one("SELECT 1 FROM dual UNION ALL SELECT 1/0 FROM dual")
      }.not_to raise_error
    end

    it "should select all results" do
      expect(plsql.select(:all, "SELECT * FROM test_employees ORDER BY employee_id")).to eq(@employees)
      expect(plsql.select("SELECT * FROM test_employees ORDER BY employee_id")).to eq(@employees)
    end

  end

end
