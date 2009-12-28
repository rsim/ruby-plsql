# encoding: utf-8

require File.dirname(__FILE__) + '/../spec_helper'

describe "Parameter type mapping /" do
  before(:all) do
    plsql.connection = get_connection
  end

  after(:all) do
    plsql.logoff
  end

  describe "Function with string parameters" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_uppercase
          ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_uppercase;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION test_uppercase"
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
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_sum
          ( p_num1 NUMBER, p_num2 NUMBER )
          RETURN NUMBER
        IS
        BEGIN
          RETURN p_num1 + p_num2;
        END test_sum;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_number_1
          ( p_num NUMBER )
          RETURN VARCHAR2
        IS
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
        END test_number_1;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_integers
          ( p_pls_int PLS_INTEGER, p_bin_int BINARY_INTEGER, x_pls_int OUT PLS_INTEGER, x_bin_int OUT BINARY_INTEGER )
        IS
        BEGIN
          x_pls_int := p_pls_int;
          x_bin_int := p_bin_int;
        END;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION test_sum"
      plsql.execute "DROP FUNCTION test_number_1"
      plsql.execute "DROP PROCEDURE test_integers"
    end
  
    it "should process integer parameters" do
      plsql.test_sum(123,456).should == 579
    end

    it "should process big integer parameters" do
      plsql.test_sum(123123123123,456456456456).should == 579579579579
    end

    it "should process float parameters and return BigDecimal" do
      plsql.test_sum(123.123,456.456).should == BigDecimal("579.579")
    end

    it "should process BigDecimal parameters and return BigDecimal" do
      plsql.test_sum(:p_num1 => BigDecimal("123.123"), :p_num2 => BigDecimal("456.456")).should == BigDecimal("579.579")
    end

    it "should process nil parameter as NULL" do
      plsql.test_sum(123,nil).should be_nil
    end

    it "should convert true value to 1 for NUMBER parameter" do
      plsql.test_number_1(true).should == 'Y'
    end

    it "should convert false value to 0 for NUMBER parameter" do
      plsql.test_number_1(false).should == 'N'
    end

    it "should process binary integer parameters" do
      plsql.test_integers(123, 456).should == {:x_pls_int => 123, :x_bin_int => 456}
    end
  end

  describe "Function with date parameters" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_date
          ( p_date DATE )
          RETURN DATE
        IS
        BEGIN
          RETURN p_date + 1;
        END test_date;
      SQL
    end
  
    before(:each) do
      plsql.default_timezone = :local
    end

    after(:all) do
      plsql.execute "DROP FUNCTION test_date"
    end
  
    it "should process Time parameters" do
      now = Time.local(2008,8,12,14,28,0)
      plsql.test_date(now).should == now + 60*60*24
    end

    it "should process UTC Time parameters" do
      plsql.default_timezone = :utc
      now = Time.utc(2008,8,12,14,28,0)
      plsql.test_date(now).should == now + 60*60*24
    end

    it "should process DateTime parameters" do
      now = DateTime.parse(Time.local(2008,8,12,14,28,0).iso8601)
      result = plsql.test_date(now)
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
    end
  
    it "should process old DateTime parameters" do
      now = DateTime.civil(1901,1,1,12,0,0,plsql.local_timezone_offset)
      result = plsql.test_date(now)
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
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
      result.class.should == Time
      result.should == Time.parse((now + 1).strftime("%c"))
    end
  
    it "should process nil date parameter as NULL" do
      plsql.test_date(nil).should be_nil
    end

  end

  describe "Function with timestamp parameters" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_timestamp
          ( p_time TIMESTAMP )
          RETURN TIMESTAMP
        IS
        BEGIN
          RETURN p_time + NUMTODSINTERVAL(1, 'DAY');
        END test_timestamp;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION test_timestamp"
    end
  
    it "should process timestamp parameters" do
      # now = Time.now
      now = Time.local(2008,8,12,14,28,0)
      plsql.test_timestamp(now).should == now + 60*60*24
    end

  end

  describe "Procedure with output parameters" do
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_copy
          ( p_from VARCHAR2, p_to OUT VARCHAR2, p_to_double OUT VARCHAR2 )
        IS
        BEGIN
          p_to := p_from;
          p_to_double := p_from || p_from;
        END test_copy;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP PROCEDURE test_copy"
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

    it "should substitute named output parameters with nil if they are not specified" do
      plsql.test_copy(:p_from => "abc").should == { :p_to => "abc", :p_to_double => "abcabc" }
    end

  end

  describe "Package with procedures with same name but different argument lists" do
    before(:all) do
      plsql.execute <<-SQL
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
      SQL
      plsql.execute <<-SQL
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
      SQL

    end
  
    after(:all) do
      plsql.execute "DROP PACKAGE test_package2"
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
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_function
          ( p_from VARCHAR2, p_to OUT VARCHAR2, p_to_double OUT VARCHAR2 )
          RETURN NUMBER
        IS
        BEGIN
          p_to := p_from;
          p_to_double := p_from || p_from;
          RETURN LENGTH(p_from);
        END test_copy_function;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION test_copy_function"
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

  end

  describe "Function or procedure without parameters" do
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_no_params
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN 'dummy';
        END test_no_params;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_proc_no_params
        IS
        BEGIN
          NULL;
        END test_proc_no_params;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION test_no_params"
      plsql.execute "DROP PROCEDURE test_proc_no_params"
    end

    it "should find function" do
      PLSQL::Procedure.find(plsql, :test_no_params).should_not be_nil
    end

    it "should return function value" do
      plsql.test_no_params.should == "dummy"
    end

    it "should find procedure" do
      PLSQL::Procedure.find(plsql, :test_proc_no_params).should_not be_nil
    end

    it "should execute procedure" do
      plsql.test_proc_no_params.should be_nil
    end

  end

  describe "Function with CLOB parameter and return value" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_clob
          ( p_clob CLOB )
          RETURN CLOB
        IS
        BEGIN
          RETURN p_clob;
        END test_clob;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION test_clob"
    end
  
    it "should find existing procedure" do
      PLSQL::Procedure.find(plsql, :test_clob).should_not be_nil
    end

    it "should execute function and return correct value" do
      large_text = 'ābčdēfghij' * 10_000
      plsql.test_clob(large_text).should == large_text
    end

    unless defined?(JRUBY_VERSION)

      it "should execute function with empty string and return nil (oci8 cannot pass empty CLOB parameter)" do
        text = ''
        plsql.test_clob(text).should be_nil
      end
    
    else

      it "should execute function with empty string and return empty string" do
        text = ''
        plsql.test_clob(text).should == text
      end
    
    end

    it "should execute function with nil and return nil" do
      plsql.test_clob(nil).should be_nil
    end

  end

  describe "Procedrue with CLOB parameter and return value" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_clob_proc
          ( p_clob CLOB,
            p_return OUT CLOB)
        IS
        BEGIN
          p_return := p_clob;
        END test_clob_proc;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP PROCEDURE test_clob_proc"
    end
  
    it "should find existing procedure" do
      PLSQL::Procedure.find(plsql, :test_clob_proc).should_not be_nil
    end

    it "should execute function and return correct value" do
      large_text = 'ābčdēfghij' * 10_000
      plsql.test_clob_proc(large_text)[:p_return].should == large_text
    end
  end

  describe "Procedrue with BLOB parameter and return value" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_blob_proc
          ( p_blob BLOB,
            p_return OUT BLOB)
        IS
        BEGIN
          p_return := p_blob;
        END test_blob_proc;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP PROCEDURE test_blob_proc"
    end
  
    it "should find existing procedure" do
      PLSQL::Procedure.find(plsql, :test_blob_proc).should_not be_nil
    end

    it "should execute function and return correct value" do
      large_binary = '\000\001\002\003\004\005\006\007\010\011' * 10_000
      plsql.test_blob_proc(large_binary)[:p_return].should == large_binary
    end
  end

  describe "Function with record parameter" do

    before(:all) do
      plsql.execute "DROP TABLE test_employees" rescue nil
      plsql.execute <<-SQL
        CREATE TABLE test_employees (
          employee_id   NUMBER(15),
          first_name    VARCHAR2(50),
          last_name     VARCHAR2(50),
          hire_date     DATE
        )
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_full_name (p_employee test_employees%ROWTYPE)
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN p_employee.first_name || ' ' || p_employee.last_name;
        END test_full_name;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_employee_record (p_employee test_employees%ROWTYPE)
          RETURN test_employees%ROWTYPE
        IS
        BEGIN
          RETURN p_employee;
        END test_employee_record;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_employee_record2 (p_employee test_employees%ROWTYPE, x_employee OUT test_employees%ROWTYPE)
          RETURN test_employees%ROWTYPE
        IS
        BEGIN
          x_employee.employee_id := p_employee.employee_id;
          x_employee.first_name := p_employee.first_name;
          x_employee.last_name := p_employee.last_name;
          x_employee.hire_date := p_employee.hire_date;
          RETURN p_employee;
        END test_employee_record2;
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
      plsql.execute "DROP FUNCTION test_full_name"
      plsql.execute "DROP FUNCTION test_employee_record"
      plsql.execute "DROP FUNCTION test_employee_record2"
      plsql.execute "DROP TABLE test_employees"
    end

    it "should find existing function" do
      PLSQL::Procedure.find(plsql, :test_full_name).should_not be_nil
    end

    it "should execute function with named parameter and return correct value" do
      plsql.test_full_name(:p_employee => @p_employee).should == 'First Last'
    end

    it "should execute function with sequential parameter and return correct value" do
      plsql.test_full_name(@p_employee).should == 'First Last'
    end

    it "should execute function with Hash parameter using strings as keys" do
      plsql.test_full_name(@p_employee2).should == 'Second Last'
    end

    it "should raise error if wrong field name is passed for record parameter" do
      lambda do
        plsql.test_full_name(@p_employee.merge :xxx => 'xxx').should == 'Second Last'
      end.should raise_error(ArgumentError)
    end

    it "should return record return value" do
      plsql.test_employee_record(@p_employee).should == @p_employee
    end

    it "should return record return value and output record parameter value" do
      plsql.test_employee_record2(@p_employee, nil).should == [@p_employee, {:x_employee => @p_employee}]
    end

  end

  describe "Function with boolean parameters" do

    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_boolean
          ( p_boolean BOOLEAN )
          RETURN BOOLEAN
        IS
        BEGIN
          RETURN p_boolean;
        END test_boolean;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_boolean2
            ( p_boolean BOOLEAN, x_boolean OUT BOOLEAN )
        IS
        BEGIN
          x_boolean := p_boolean;
        END test_boolean2;
      SQL
    end

    after(:all) do
      plsql.execute "DROP FUNCTION test_boolean"
      plsql.execute "DROP PROCEDURE test_boolean2"
    end

    it "should accept true value and return true value" do
      plsql.test_boolean(true).should == true
    end

    it "should accept false value and return false value" do
      plsql.test_boolean(false).should == false
    end

    it "should accept nil value and return nil value" do
      plsql.test_boolean(nil).should be_nil
    end

    it "should accept true value and assign true value to output parameter" do
      plsql.test_boolean2(true, nil).should == {:x_boolean => true}
    end

    it "should accept false value and assign false value to output parameter" do
      plsql.test_boolean2(false, nil).should == {:x_boolean => false}
    end

    it "should accept nil value and assign nil value to output parameter" do
      plsql.test_boolean2(nil, nil).should == {:x_boolean => nil}
    end

  end

  describe "Function with object type parameter" do

    before(:all) do
      plsql.execute "DROP TYPE t_employee" rescue nil
      plsql.execute "DROP TYPE t_phones" rescue nil
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_address AS OBJECT (
          street    VARCHAR2(50),
          city      VARCHAR2(50),
          country   VARCHAR2(50)
        )
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_phone AS OBJECT (
          type            VARCHAR2(10),
          phone_number    VARCHAR2(50)
        )
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_phones AS TABLE OF T_PHONE
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
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_full_name (p_employee t_employee)
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN p_employee.first_name || ' ' || p_employee.last_name;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_employee_object (p_employee t_employee)
          RETURN t_employee
        IS
        BEGIN
          RETURN p_employee;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_employee_object2 (p_employee t_employee, x_employee OUT t_employee)
          RETURN t_employee
        IS
        BEGIN
          x_employee := p_employee;
          RETURN p_employee;
        END;
      SQL
      @p_employee = {
        :employee_id => 1,
        :first_name => 'First',
        :last_name => 'Last',
        :hire_date => Time.local(2000,01,31),
        :address => {:street => 'Main street 1', :city => 'Riga', :country => 'Latvia'},
        :phones => [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      }
    end

    after(:all) do
      plsql.execute "DROP FUNCTION test_full_name"
      plsql.execute "DROP FUNCTION test_employee_object"
      plsql.execute "DROP FUNCTION test_employee_object2"
      plsql.execute "DROP TYPE t_employee"
      plsql.execute "DROP TYPE t_address"
      plsql.execute "DROP TYPE t_phones"
      plsql.execute "DROP TYPE t_phone"
    end

    it "should find existing function" do
      PLSQL::Procedure.find(plsql, :test_full_name).should_not be_nil
    end

    it "should execute function with named parameter and return correct value" do
      plsql.test_full_name(:p_employee => @p_employee).should == 'First Last'
    end

    it "should execute function with sequential parameter and return correct value" do
      plsql.test_full_name(@p_employee).should == 'First Last'
    end

    it "should raise error if wrong field name is passed for record parameter" do
      lambda do
        plsql.test_full_name(@p_employee.merge :xxx => 'xxx')
      end.should raise_error(ArgumentError)
    end

    it "should return object type return value" do
      plsql.test_employee_object(@p_employee).should == @p_employee
    end

    it "should return object type return value and output object type parameter value" do
      plsql.test_employee_object2(@p_employee, nil).should == [@p_employee, {:x_employee => @p_employee}]
    end

  end


  describe "Function with table parameter" do

    before(:all) do
      # Array of numbers
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_numbers AS TABLE OF NUMBER(15)
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_sum (p_numbers IN t_numbers)
          RETURN NUMBER
        IS
          l_sum   NUMBER(15) := 0;
        BEGIN
          IF p_numbers.COUNT > 0 THEN
            FOR i IN p_numbers.FIRST..p_numbers.LAST LOOP
              IF p_numbers.EXISTS(i) THEN
                l_sum := l_sum + p_numbers(i);
              END IF;
            END LOOP;
            RETURN l_sum;
          ELSE
            RETURN NULL;
          END IF;
        END;
      SQL

      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_increment(p_numbers IN t_numbers, p_increment_by IN NUMBER DEFAULT 1)
          RETURN t_numbers
        IS
          l_numbers t_numbers := t_numbers();
        BEGIN
          FOR i IN p_numbers.FIRST..p_numbers.LAST LOOP
            IF p_numbers.EXISTS(i) THEN
              l_numbers.EXTEND;
              l_numbers(i) := p_numbers(i) + p_increment_by;
            END IF;
          END LOOP;
          RETURN l_numbers;
        END;
      SQL

      # Array of strings
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_strings AS TABLE OF VARCHAR2(4000)
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_strings(p_strings IN t_strings, x_strings OUT t_strings)
          RETURN t_strings
        IS
        BEGIN
          x_strings := t_strings();
          FOR i IN p_strings.FIRST..p_strings.LAST LOOP
            IF p_strings.EXISTS(i) THEN
              x_strings.EXTEND;
              x_strings(i) := p_strings(i);
            END IF;
          END LOOP;
          RETURN x_strings;
        END;
      SQL

      # Wrong type definition inside package
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_collections IS
          TYPE t_numbers IS TABLE OF NUMBER(15);
          FUNCTION test_sum (p_numbers IN t_numbers)
            RETURN NUMBER;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_collections IS
          FUNCTION test_sum (p_numbers IN t_numbers)
          RETURN NUMBER
          IS
            l_sum   NUMBER(15) := 0;
          BEGIN
            IF p_numbers.COUNT > 0 THEN
              FOR i IN p_numbers.FIRST..p_numbers.LAST LOOP
                IF p_numbers.EXISTS(i) THEN
                  l_sum := l_sum + p_numbers(i);
                END IF;
              END LOOP;
              RETURN l_sum;
            ELSE
              RETURN NULL;
            END IF;
          END;
        END;
      SQL

      # Array of objects
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_phone AS OBJECT (
          type            VARCHAR2(10),
          phone_number    VARCHAR2(50)
        )
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_phones AS TABLE OF T_PHONE
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_objects(p_phones IN t_phones, x_phones OUT t_phones)
          RETURN t_phones
        IS
        BEGIN
          x_phones := p_phones;
          RETURN x_phones;
        END;
      SQL
    end

    after(:all) do
      plsql.execute "DROP FUNCTION test_sum"
      plsql.execute "DROP FUNCTION test_increment"
      plsql.execute "DROP FUNCTION test_copy_strings"
      plsql.execute "DROP PACKAGE test_collections"
      plsql.execute "DROP FUNCTION test_copy_objects"
      plsql.execute "DROP TYPE t_numbers"
      plsql.execute "DROP TYPE t_strings"
      plsql.execute "DROP TYPE t_phones"
      plsql.execute "DROP TYPE t_phone"
    end

    it "should find existing function" do
      PLSQL::Procedure.find(plsql, :test_sum).should_not be_nil
    end

    it "should execute function with number array parameter" do
      plsql.test_sum([1,2,3,4]).should == 10
    end

    it "should return number array return value" do
      plsql.test_increment([1,2,3,4], 1).should == [2,3,4,5]
    end

    it "should execute function with string array and return string array output parameter" do
      strings = ['1','2','3','4']
      plsql.test_copy_strings(strings).should == [strings, {:x_strings => strings}]
    end

    it "should raise error if parameter type is defined inside package" do
      lambda do
        plsql.test_collections.test_sum([1,2,3,4])
      end.should raise_error(ArgumentError)
    end

    it "should execute function with object array and return object array output parameter" do
      phones = [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      plsql.test_copy_objects(phones).should == [phones, {:x_phones => phones}]
    end

    it "should execute function with empty object array" do
      phones = []
      plsql.test_copy_objects(phones).should == [phones, {:x_phones => phones}]
    end

  end

  describe "Function with VARRAY parameter" do

    before(:all) do
      # Array of numbers
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_numbers_array AS VARRAY(100) OF NUMBER(15)
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_sum (p_numbers IN t_numbers_array)
          RETURN NUMBER
        IS
          l_sum   NUMBER(15) := 0;
        BEGIN
          IF p_numbers.COUNT > 0 THEN
            FOR i IN p_numbers.FIRST..p_numbers.LAST LOOP
              l_sum := l_sum + p_numbers(i);
            END LOOP;
            RETURN l_sum;
          ELSE
            RETURN NULL;
          END IF;
        END;
      SQL
    
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_increment(p_numbers IN t_numbers_array, p_increment_by IN NUMBER DEFAULT 1)
          RETURN t_numbers_array
        IS
          l_numbers t_numbers_array := t_numbers_array();
        BEGIN
          FOR i IN p_numbers.FIRST..p_numbers.LAST LOOP
            l_numbers.EXTEND;
            l_numbers(i) := p_numbers(i) + p_increment_by;
          END LOOP;
          RETURN l_numbers;
        END;
      SQL
    
      # Array of strings
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_strings_array AS VARRAY(100) OF VARCHAR2(4000)
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_strings(p_strings IN t_strings_array, x_strings OUT t_strings_array)
          RETURN t_strings_array
        IS
        BEGIN
          x_strings := t_strings_array();
          FOR i IN p_strings.FIRST..p_strings.LAST LOOP
            x_strings.EXTEND;
            x_strings(i) := p_strings(i);
          END LOOP;
          RETURN x_strings;
        END;
      SQL

      # Array of objects
      plsql.execute "DROP TYPE t_phones_array" rescue nil
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_phone AS OBJECT (
          type            VARCHAR2(10),
          phone_number    VARCHAR2(50)
        )
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_phones_array AS ARRAY(100) OF T_PHONE
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_copy_objects(p_phones IN t_phones_array, x_phones OUT t_phones_array)
          RETURN t_phones_array
        IS
        BEGIN
          x_phones := p_phones;
          RETURN x_phones;
        END;
      SQL
    end

    after(:all) do
      plsql.execute "DROP FUNCTION test_sum"
      plsql.execute "DROP FUNCTION test_increment"
      plsql.execute "DROP FUNCTION test_copy_strings"
      plsql.execute "DROP FUNCTION test_copy_objects"
      plsql.execute "DROP TYPE t_numbers_array"
      plsql.execute "DROP TYPE t_strings_array"
      plsql.execute "DROP TYPE t_phones_array"
      plsql.execute "DROP TYPE t_phone"
    end

    it "should find existing function" do
      PLSQL::Procedure.find(plsql, :test_sum).should_not be_nil
    end

    it "should execute function with number array parameter" do
      plsql.test_sum([1,2,3,4]).should == 10
    end

    it "should return number array return value" do
      plsql.test_increment([1,2,3,4], 1).should == [2,3,4,5]
    end

    it "should execute function with string array and return string array output parameter" do
      strings = ['1','2','3','4']
      plsql.test_copy_strings(strings).should == [strings, {:x_strings => strings}]
    end

    it "should execute function with object array and return object array output parameter" do
      phones = [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      plsql.test_copy_objects(phones).should == [phones, {:x_phones => phones}]
    end

    it "should execute function with empty object array" do
      phones = []
      plsql.test_copy_objects(phones).should == [phones, {:x_phones => phones}]
    end

  end

  describe "Function with cursor return value or parameter" do

    before(:all) do
      plsql.execute "DROP TABLE test_employees" rescue nil
      plsql.execute <<-SQL
        CREATE TABLE test_employees (
          employee_id   NUMBER(15),
          first_name    VARCHAR2(50),
          last_name     VARCHAR2(50),
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
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_cursor
          RETURN SYS_REFCURSOR
        IS
          l_cursor  SYS_REFCURSOR;
        BEGIN
          OPEN l_cursor FOR
          SELECT * FROM test_employees ORDER BY employee_id;
          RETURN l_cursor;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_cursor_out(x_cursor OUT SYS_REFCURSOR)
        IS
        BEGIN
          OPEN x_cursor FOR
          SELECT * FROM test_employees ORDER BY employee_id;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_cursor_fetch(p_cursor SYS_REFCURSOR)
          RETURN test_employees%ROWTYPE
        IS
          l_record  test_employees%ROWTYPE;
        BEGIN
          FETCH p_cursor INTO l_record;
          RETURN l_record;
        END;
      SQL
      @fields = [:employee_id, :first_name, :last_name, :hire_date]
      @employees = (1..10).map do |i|
        {
          :employee_id => i,
          :first_name => "First #{i}",
          :last_name => "Last #{i}",
          :hire_date => Time.local(2000,01,i)
        }
      end
      @employees.each do |e|
        plsql.test_insert_employee(e)
      end
      plsql.connection.commit
    end

    after(:all) do
      plsql.execute "DROP FUNCTION test_cursor"
      plsql.execute "DROP PROCEDURE test_cursor_out"
      plsql.execute "DROP PROCEDURE test_insert_employee"
      plsql.execute "DROP FUNCTION test_cursor_fetch"
      plsql.execute "DROP TABLE test_employees"
    end

    it "should find existing function" do
      PLSQL::Procedure.find(plsql, :test_cursor).should_not be_nil
    end

    it "should return cursor and fetch first row" do
      plsql.test_cursor do |cursor|
        cursor.fetch.should == @fields.map{|f| @employees[0][f]}
      end.should be_nil
    end

    it "should close all returned cursors after block is executed" do
      cursor2 = nil
      plsql.test_cursor do |cursor|
        cursor2 = cursor
      end.should be_nil
      lambda { cursor2.fetch }.should raise_error
    end

    it "should not raise error if cursor is closed inside block" do
      lambda do
        plsql.test_cursor do |cursor|
          cursor.close
        end
      end.should_not raise_error
    end

    it "should fetch hash from returned cursor" do
      plsql.test_cursor do |cursor|
        cursor.fetch_hash.should == @employees[0]
      end
    end

    it "should fetch all rows from returned cursor" do
      plsql.test_cursor do |cursor|
        cursor.fetch_all.should == @employees.map{|e| @fields.map{|f| e[f]}}
      end
    end

    it "should fetch all rows as hash from returned cursor" do
      plsql.test_cursor do |cursor|
        cursor.fetch_hash_all.should == @employees
      end
    end

    it "should get field names from returned cursor" do
      plsql.test_cursor do |cursor|
        cursor.fields.should == @fields
      end
    end

    it "should return output parameter with cursor and fetch first row" do
      plsql.test_cursor_out do |result|
        result[:x_cursor].fetch.should == @fields.map{|f| @employees[0][f]}
      end.should be_nil
    end

    it "should return output parameter with cursor and fetch all rows as hash" do
      plsql.test_cursor_out do |result|
        result[:x_cursor].fetch_hash_all.should == @employees
      end.should be_nil
    end

    it "should execute function with cursor parameter and return record" do
      pending "not possible from JDBC" if defined?(JRUBY_VERSION)
      plsql.test_cursor do |cursor|
        plsql.test_cursor_fetch(cursor).should == @employees[0]
      end
    end

  end
end

describe "Synonyms /" do

  before(:all) do
    plsql.connection = get_connection
  end

  after(:all) do
    plsql.logoff
  end

  describe "Local synonym to function" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION hr.test_uppercase
          ( p_string VARCHAR2 )
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN UPPER(p_string);
        END test_uppercase;
      SQL
      plsql.execute "CREATE SYNONYM test_synonym FOR hr.test_uppercase"
    end
  
    after(:all) do
      plsql.execute "DROP SYNONYM test_synonym"
      plsql.execute "DROP FUNCTION hr.test_uppercase"
    end
  
    it "should find synonym to function" do
      PLSQL::Procedure.find(plsql, :test_synonym).should_not be_nil
    end

    it "should execute function using synonym and return correct value" do
      plsql.test_synonym('xxx').should == 'XXX'
    end

  end

  describe "Public synonym to function" do
  
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION hr.test_ora_login_user
          RETURN VARCHAR2
        IS
        BEGIN
          RETURN 'XXX';
        END test_ora_login_user;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION hr.test_ora_login_user"
    end
  
    it "should find public synonym to function" do
      PLSQL::Procedure.find(plsql, :ora_login_user).should_not be_nil
    end

    it "should execute function using public synonym and return correct value" do
      plsql.ora_login_user.should == 'HR'
    end

    it "should not find public synonym if schema prefix is used" do
      lambda { plsql.hr.ora_login_user }.should raise_error(ArgumentError)
    end

    it "should find private synonym before public synonym" do
      # should reconnect to force clearing of procedure cache
      plsql.connection = get_connection
      plsql.execute "DROP SYNONYM ora_login_user" rescue nil
      plsql.execute "CREATE SYNONYM ora_login_user FOR hr.test_ora_login_user"
      plsql.ora_login_user.should == 'XXX'
      plsql.execute "DROP SYNONYM ora_login_user"
      plsql.connection = get_connection
      plsql.ora_login_user.should == 'HR'
    end

  end

end
