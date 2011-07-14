# encoding: utf-8

require 'spec_helper'

describe "Parameter type mapping /" do
  
  before(:all) do
    plsql(:pg).connect! PG_CONNECTION_PARAMS
  end

  after(:all) do
    plsql(:pg).logoff
  end

  describe "Function with string parameters" do
  
    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_uppercase(p_string varchar)
          RETURNS varchar
        AS $$
        BEGIN
          RETURN upper(p_string);
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end
  
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_uppercase(varchar)"
    end
  
    it "should find existing function" do
      PLSQL::Procedure.find(plsql(:pg), :test_uppercase).should_not be_nil
    end

    it "should not find nonexisting function" do
      PLSQL::Procedure.find(plsql(:pg), :qwerty123456).should be_nil
    end

    it "should execute function and return correct value" do
      plsql(:pg).test_uppercase('xxx').should == 'XXX'
    end

    it "should execute function with named parameters and return correct value" do
      plsql(:pg).test_uppercase(:p_string => 'xxx').should == 'XXX'
    end

    it "should raise error if wrong number of arguments is passed" do
      lambda { plsql(:pg).test_uppercase('xxx','yyy') }.should raise_error(ArgumentError)
    end

    it "should raise error if wrong named argument is passed" do
      lambda { plsql(:pg).test_uppercase(:p_string2 => 'xxx') }.should raise_error(ArgumentError)
    end
  
    it "should execute function with schema name specified" do
      plsql(:pg).hr.test_uppercase('xxx').should == 'XXX'
    end

    it "should process nil parameter as NULL" do
      plsql(:pg).test_uppercase(nil).should be_nil
    end

  end
  
  describe "Function with numeric parameters" do
  
    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_sum(p_num1 numeric, p_num2 numeric)
          RETURNS numeric
        AS $$
        BEGIN
          RETURN p_num1 + p_num2;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_number_1(p_num numeric)
          RETURNS varchar
        AS $$
        BEGIN
          IF p_num = 1 THEN
            RETURN 'Y';
          ELSIF p_num = 0 THEN
            RETURN 'N';
          ELSIF p_num IS NULL THEN
            RETURN NULL;
          ELSE
            RETURN 'UNKNOWN';
          END IF;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_integers(p_pls_int integer, p_bin_int integer, OUT x_pls_int integer, OUT x_bin_int integer)
        AS $$
        BEGIN
          x_pls_int := p_pls_int;
          x_bin_int := p_bin_int;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end
  
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_sum(numeric, numeric)"
      plsql(:pg).execute "DROP FUNCTION test_number_1(numeric)"
      plsql(:pg).execute "DROP FUNCTION test_integers(integer, integer)"
    end
  
    it "should process integer parameters" do
      plsql(:pg).test_sum(123, 456).should == 579
    end

    it "should process big integer parameters" do
      plsql(:pg).test_sum(123123123123, 456456456456).should == 579579579579
    end

    it "should process float parameters and return BigDecimal" do
      plsql(:pg).test_sum(123.123, 456.456).should == BigDecimal("579.579")
    end

    it "should process BigDecimal parameters and return BigDecimal" do
      plsql(:pg).test_sum(:p_num1 => BigDecimal("123.123"), :p_num2 => BigDecimal("456.456")).should == BigDecimal("579.579")
    end

    it "should process nil parameter as NULL" do
      plsql(:pg).test_sum(123, nil).should be_nil
    end

    it "should convert true value to 1 for NUMBER parameter" do
      plsql(:pg).test_number_1(true).should == 'Y'
    end

    it "should convert false value to 0 for NUMBER parameter" do
      plsql(:pg).test_number_1(false).should == 'N'
    end

    it "should process integer parameters" do
      plsql(:pg).test_integers(123, 456).should == {:x_pls_int => 123, :x_bin_int => 456}
    end
  end
  
  describe "Function with date parameters" do
  
    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_date(p_date timestamp with time zone)
          RETURNS timestamp with time zone
        AS $$
        BEGIN
          RETURN p_date + INTERVAL '1 day';
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end
  
    before(:each) do
      plsql(:pg).default_timezone = :local
    end

    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_date(timestamp with time zone)"
    end
  
    it "should process Time parameters" do
      now = Time.local(2008, 8, 12, 14, 28, 0)
      plsql(:pg).test_date(now).should == now + 60 * 60 * 24
    end

    it "should process UTC Time parameters" do
      plsql(:pg).default_timezone = :utc
      now = Time.utc(2008, 8, 12, 14, 28, 0)
      plsql(:pg).test_date(now).should == now + 60 * 60 * 24
    end

    it "should process DateTime parameters" do
      now = DateTime.parse(Time.local(2008, 8, 12, 14, 28, 0).iso8601)
      result = plsql(:pg).test_date(now)
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
    end
  
    it "should process old DateTime parameters" do
      now = DateTime.civil(1901, 1, 1, 12, 0, 0, plsql(:pg).local_timezone_offset)
      result = plsql(:pg).test_date(now)
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
    end

    it "should process Date parameters" do
      now = Date.new(2008, 8, 12)
      result = plsql(:pg).test_date(now)
      result.class.should == Time    
      result.should == Time.parse((now + 1).strftime("%c"))
    end
  
    it "should process old Date parameters" do
      now = Date.new(1901, 1, 1)
      result = plsql(:pg).test_date(now)
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
    end
  
    it "should process nil date parameter as NULL" do
      plsql(:pg).test_date(nil).should be_nil
    end

  end
  
  describe "Function with output parameters" do
    
    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy(p_from varchar, OUT p_to varchar, OUT p_to_double varchar)
        AS $$
        BEGIN
          p_to := p_from;
          p_to_double := p_from || p_from;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end
  
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_copy(varchar)"
    end
  
    it "should return hash with output parameters" do
      plsql(:pg).test_copy("abc", nil, nil).should == { :p_to => "abc", :p_to_double => "abcabc" }
    end

    it "should return hash with output parameters when called with named parameters" do
      pending "Need to implement this."
      #plsql(:pg).test_copy(:p_from => "abc", :p_to => nil, :p_to_double => nil).should == { :p_to => "abc", :p_to_double => "abcabc" }
    end

    it "should substitute output parameters with nil if they are not specified" do
      plsql(:pg).test_copy("abc").should == { :p_to => "abc", :p_to_double => "abcabc" }
    end

    it "should substitute named output parameters with nil if they are not specified" do
      pending "Need to implement this."
      #plsql(:pg).test_copy(:p_from => "abc").should == { :p_to => "abc", :p_to_double => "abcabc" }
    end

  end
  
  describe "Functions with same name but different argument lists" do
    
    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_function(p_string varchar)
          RETURNS varchar
        AS $$
        BEGIN
          RETURN upper(p_string);
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_function(p_string varchar, p_string2 varchar)
          RETURNS varchar
        AS $$
        BEGIN
          RETURN p_string || p_string2;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_function(p_number numeric, OUT p_result varchar)
        AS $$
        BEGIN
          p_result := trim(to_char(p_number, '999'));
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_function(p_number numeric, p_number2 numeric, OUT p_result numeric)
        AS $$
        BEGIN
          p_result := p_number + p_number2;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_function2(p_string varchar)
          RETURNS varchar
        AS $$
        BEGIN
          RETURN upper(p_string);
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_function3(p_string varchar, p_string2 varchar DEFAULT ' ')
          RETURNS varchar
        AS $$
        BEGIN
          RETURN p_string || p_string2;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_function3(p_number numeric, p_number2 numeric, OUT p_result numeric)
        AS $$
        BEGIN
          p_result := p_number + p_number2;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL

    end
  
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_function(varchar)"
      plsql(:pg).execute "DROP FUNCTION test_function(varchar, varchar)"
      plsql(:pg).execute "DROP FUNCTION test_function(numeric)"
      plsql(:pg).execute "DROP FUNCTION test_function(numeric, numeric)"
      plsql(:pg).execute "DROP FUNCTION test_function2(varchar)"
      plsql(:pg).execute "DROP FUNCTION test_function3(varchar, varchar)"
      plsql(:pg).execute "DROP FUNCTION test_function3(numeric, numeric)"
    end

    it "should identify overloaded function definition" do
      @procedure = PLSQL::Procedure.find(plsql(:pg), :test_function)
      @procedure.should_not be_nil
      @procedure.should be_overloaded
    end

    it "should identify non-overloaded function definition" do
      @procedure = PLSQL::Procedure.find(plsql(:pg), :test_function2)
      @procedure.should_not be_nil
      @procedure.should_not be_overloaded
    end

    it "should execute correct functions based on number of arguments and return correct value" do
      plsql(:pg).test_function('xxx').should == 'XXX'
      plsql(:pg).test_function('xxx', 'xxx').should == "xxxxxx"
    end

    it "should execute correct functions based on number of named arguments and return correct value" do
      plsql(:pg).test_function(:p_string => 'xxx').should == 'XXX'
      plsql(:pg).test_function(:p_string => 'xxx', :p_string2 => 'xxx').should == 'xxxxxx'
    end

    it "should raise exception if procedure cannot be found based on number of arguments" do
      lambda { plsql(:pg).test_function }.should raise_error(/no function matches the given name and argument types/i)
    end
  
    it "should find procedure based on types of arguments" do
      plsql(:pg).test_function(111).should == {:p_result => '111'}
      plsql(:pg).test_function(111, 111).should == {:p_result => 222}
    end

    it "should find function based on names of named arguments" do
      pending "Need to implement this."
      #plsql(:pg).test_function(:p_number => 111, :p_result => nil).should == {:p_result => '111'}
    end

    it "should find matching procedure based on partial list of named arguments" do
      plsql(:pg).test_function3(:p_string => 'xxx').should == 'xxx '
      plsql(:pg).test_function3(:p_string => 'xxx', :p_string2 => 'xxx').should == 'xxxxxx'
      pending "Make the following test work."
      #plsql(:pg).test_function3(:p_number => 1).should == 2
    end

  end
  
  describe "Function with input and output parameters" do
    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_function(INOUT p_from varchar, OUT p_to varchar, OUT p_to_double varchar)
        AS $$
        BEGIN
          p_to := p_from;
          p_to_double := p_from || p_from;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end
  
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_copy_function(varchar)"
    end
  
    it "should return hash of input and output parameters" do
      plsql(:pg).test_copy_function("abc", nil, nil).should == { :p_from => "abc", :p_to => "abc", :p_to_double => "abcabc" }
    end

    it "should return hash of input and output parameters when called with named parameters" do
      pending "Need to implement this."
      #plsql(:pg).test_copy_function(:p_from => "abc", :p_to => nil, :p_to_double => nil).should == { :p_from => "abc", :p_to => "abc", :p_to_double => "abcabc" }
    end

    it "should substitute output parameters with nil if they are not specified" do
      plsql(:pg).test_copy_function("abc").should == { :p_from => "abc", :p_to => "abc", :p_to_double => "abcabc" }
    end

  end
  
  describe "Function without parameters" do
    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_no_params()
          RETURNS varchar
        AS $$
        BEGIN
          RETURN 'dummy';
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end
  
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_no_params()"
    end

    it "should find function" do
      PLSQL::Procedure.find(plsql(:pg), :test_no_params).should_not be_nil
    end

    it "should return function value" do
      plsql(:pg).test_no_params.should == "dummy"
    end

  end
  
  describe "Function with CLOB parameter and return value" do
    
  end
  
  describe "Function with BLOB parameter and return value" do
    
  end
  
  describe "Function with record parameter" do
    
    before(:all) do
      plsql(:pg).execute "DROP TABLE test_employees" rescue nil
      plsql(:pg).execute <<-SQL
        CREATE TABLE test_employees (
          employee_id   integer,
          first_name    varchar,
          last_name     varchar,
          hire_date     timestamp with time zone
        )
      SQL
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_full_name(p_employee test_employees)
          RETURNS varchar
        AS $$
        BEGIN
          RETURN p_employee.first_name || ' ' || p_employee.last_name;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_employee_record(p_employee test_employees)
          RETURNS test_employees
        AS $$
        BEGIN
          RETURN p_employee;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_employee_record2(p_employee test_employees, INOUT x_employee test_employees)
          RETURNS test_employees
        AS $$
        BEGIN
          x_employee.employee_id := p_employee.employee_id;
          x_employee.first_name := p_employee.first_name;
          x_employee.last_name := p_employee.last_name;
          x_employee.hire_date := p_employee.hire_date;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      @p_employee = {
        :employee_id => 1,
        :first_name => 'First',
        :last_name => 'Last',
        :hire_date => Time.local(2000,01,31)
      }
      @p_employee2 = {
        'employee_id' => 1,
        'FIRST_NAME' => 'Second',
        'last_name' => 'Last',
        'hire_date' => Time.local(2000,01,31)
      }
    end
    
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_full_name(test_employees)"
      plsql(:pg).execute "DROP FUNCTION test_employee_record(test_employees)"
      plsql(:pg).execute "DROP FUNCTION test_employee_record2(test_employees, test_employees)"
      plsql(:pg).execute "DROP TABLE test_employees"
    end
    
    it "should find existing function" do
      PLSQL::Procedure.find(plsql(:pg), :test_full_name).should_not be_nil
    end

    it "should execute function with named parameter and return correct value" do
      plsql(:pg).test_full_name(:p_employee => @p_employee).should == 'First Last'
    end

    it "should execute function with sequential parameter and return correct value" do
      plsql(:pg).test_full_name(@p_employee).should == 'First Last'
    end

    it "should execute function with Hash parameter using strings as keys" do
      plsql(:pg).test_full_name(@p_employee2).should == 'Second Last'
    end

    it "should raise error if wrong field name is passed for record parameter" do
      lambda do
        plsql(:pg).test_full_name(@p_employee.merge(:xxx => 'xxx')).should == 'Second Last'
      end.should raise_error(ArgumentError)
    end

    it "should return record return value" do
      plsql(:pg).test_employee_record(@p_employee).should == @p_employee
    end

    it "should return record return value and output record parameter value" do
      plsql(:pg).test_employee_record2(@p_employee, @p_employee2).should == {:x_employee => @p_employee}
    end
    
  end
  
  describe "Function with boolean parameters" do

    before(:all) do
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_boolean(p_boolean boolean)
          RETURNS boolean
        AS $$
        BEGIN
          RETURN p_boolean;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_boolean2(p_boolean boolean, OUT x_boolean boolean)
        AS $$
        BEGIN
          x_boolean := p_boolean;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end

    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_boolean(boolean)"
      plsql(:pg).execute "DROP FUNCTION test_boolean2(boolean)"
    end

    it "should accept true value and return true value" do
      plsql(:pg).test_boolean(true).should == true
    end

    it "should accept false value and return false value" do
      plsql(:pg).test_boolean(false).should == false
    end

    it "should accept nil value and return nil value" do
      plsql(:pg).test_boolean(nil).should be_nil
    end

    it "should accept true value and assign true value to output parameter" do
      plsql(:pg).test_boolean2(true, nil).should == {:x_boolean => true}
    end

    it "should accept false value and assign false value to output parameter" do
      plsql(:pg).test_boolean2(false, nil).should == {:x_boolean => false}
    end

    it "should accept nil value and assign nil value to output parameter" do
      plsql(:pg).test_boolean2(nil, nil).should == {:x_boolean => nil}
    end

  end
  
  describe "Function with custom type parameter" do
    
  end
  
  describe "Function with array parameter" do
    
    before(:all) do
      # Array of numbers
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_sum(p_numbers numeric[])
          RETURNS numeric
        AS $$
        DECLARE
          l_sum numeric := 0;
        BEGIN
          IF array_dims(p_numbers) IS NOT NULL THEN
            FOR i IN array_lower(p_numbers, 1) .. array_upper(p_numbers, 1) LOOP
              l_sum := l_sum + p_numbers[i];
            END LOOP;
            RETURN l_sum;
          ELSE
            RETURN NULL;
          END IF;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_increment(p_numbers numeric[], p_increment_by numeric DEFAULT 1)
          RETURNS numeric[]
        AS $$
        DECLARE
          l_numbers numeric[];
        BEGIN
          FOR i IN array_lower(p_numbers, 1) .. array_upper(p_numbers, 1) LOOP
            l_numbers[i] := p_numbers[i] + p_increment_by;
          END LOOP;
          RETURN l_numbers;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    
      # Array of strings
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_strings(p_strings character varying[], OUT x_strings character varying[])
        AS $$
        BEGIN
          FOR i IN array_lower(p_strings, 1) .. array_upper(p_strings, 1) LOOP
            x_strings[i] := p_strings[i];
          END LOOP;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL

      # Array of objects
      plsql(:pg).execute "DROP TYPE t_phone" rescue nil
      plsql(:pg).execute <<-SQL
        CREATE TYPE t_phone AS (
          type            varchar,
          phone_number    varchar
        )
      SQL
      plsql(:pg).execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_objects(p_phones t_phone[], OUT x_phones t_phone[])
        AS $$
        BEGIN
          x_phones := p_phones;
          RETURN;
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end

    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_sum(numeric[])"
      plsql(:pg).execute "DROP FUNCTION test_increment(numeric[], numeric)"
      plsql(:pg).execute "DROP FUNCTION test_copy_strings(character varying[])"
      plsql(:pg).execute "DROP FUNCTION test_copy_objects(t_phone[])"
      plsql(:pg).execute "DROP TYPE t_phone"
    end

    it "should find existing function" do
      PLSQL::Procedure.find(plsql(:pg), :test_sum).should_not be_nil
    end

    it "should execute function with number array parameter" do
      plsql(:pg).test_sum([1,2,3,4]).should == 10
    end

    it "should return number array return value" do
      plsql(:pg).test_increment([1,2,3,4], 1).should == [2,3,4,5]
    end

    it "should execute function with string array and return string array output parameter" do
      strings = ['1','2','3','4']
      plsql(:pg).test_copy_strings(strings).should == {:x_strings => strings}
    end

    it "should execute function with object array and return object array output parameter" do
      phones = [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      plsql(:pg).test_copy_objects(phones).should == {:x_phones => phones}
    end

    it "should execute function with empty object array" do
      phones = []
      plsql(:pg).test_copy_objects(phones).should == {:x_phones => phones}
    end
    
  end
  
  describe "Function with cursor return value or parameter" do
    
  end
  
end