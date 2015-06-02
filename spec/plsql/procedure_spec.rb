# encoding: utf-8

require 'spec_helper'

describe "Parameter type mapping /" do
  describe "Function with string parameters" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should find existing procedure" do
      expect(PLSQL::Procedure.find(plsql, :test_uppercase)).not_to be_nil
    end

    it "should not find nonexisting procedure" do
      expect(PLSQL::Procedure.find(plsql, :qwerty123456)).to be_nil
    end

    it "should execute function and return correct value" do
      expect(plsql.test_uppercase('xxx')).to eq('XXX')
    end

    it "should execute function with named parameters and return correct value" do
      expect(plsql.test_uppercase(:p_string => 'xxx')).to eq('XXX')
    end

    it "should raise error if wrong number of arguments is passed" do
      expect { plsql.test_uppercase('xxx','yyy') }.to raise_error(ArgumentError)
    end

    it "should raise error if wrong named argument is passed" do
      expect { plsql.test_uppercase(:p_string2 => 'xxx') }.to raise_error(ArgumentError)
    end
  
    it "should execute function with schema name specified" do
      expect(plsql.hr.test_uppercase('xxx')).to eq('XXX')
    end

    it "should process nil parameter as NULL" do
      expect(plsql.test_uppercase(nil)).to be_nil
    end

  end

  describe "Function with numeric parameters" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should process integer parameters" do
      expect(plsql.test_sum(123,456)).to eq(579)
    end

    it "should process big integer parameters" do
      expect(plsql.test_sum(123123123123,456456456456)).to eq(579579579579)
    end

    it "should process float parameters and return BigDecimal" do
      expect(plsql.test_sum(123.123,456.456)).to eq(BigDecimal("579.579"))
    end

    it "should process BigDecimal parameters and return BigDecimal" do
      expect(plsql.test_sum(:p_num1 => BigDecimal("123.123"), :p_num2 => BigDecimal("456.456"))).to eq(BigDecimal("579.579"))
    end

    it "should process nil parameter as NULL" do
      expect(plsql.test_sum(123,nil)).to be_nil
    end

    it "should convert true value to 1 for NUMBER parameter" do
      expect(plsql.test_number_1(true)).to eq('Y')
    end

    it "should convert false value to 0 for NUMBER parameter" do
      expect(plsql.test_number_1(false)).to eq('N')
    end

    it "should process binary integer parameters" do
      expect(plsql.test_integers(123, 456)).to eq({:x_pls_int => 123, :x_bin_int => 456})
    end
  end

  describe "Function with date parameters" do
  
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should process Time parameters" do
      now = Time.local(2008,8,12,14,28,0)
      expect(plsql.test_date(now)).to eq(now + 60*60*24)
    end

    it "should process UTC Time parameters" do
      plsql.default_timezone = :utc
      now = Time.utc(2008,8,12,14,28,0)
      expect(plsql.test_date(now)).to eq(now + 60*60*24)
    end

    it "should process DateTime parameters" do
      now = DateTime.parse(Time.local(2008,8,12,14,28,0).iso8601)
      result = plsql.test_date(now)
      expect(result.class).to eq(Time)
      expect(result).to eq(Time.parse((now + 1).strftime("%c")))
    end
  
    it "should process old DateTime parameters" do
      now = DateTime.civil(1901,1,1,12,0,0,plsql.local_timezone_offset)
      result = plsql.test_date(now)
      expect(result.class).to eq(Time)
      expect(result).to eq(Time.parse((now + 1).strftime("%c")))
    end

    it "should process Date parameters" do
      now = Date.new(2008,8,12)
      result = plsql.test_date(now)
      expect(result.class).to eq(Time)
      expect(result).to eq(Time.parse((now + 1).strftime("%c")))
    end
  
    it "should process old Date parameters" do
      now = Date.new(1901,1,1)
      result = plsql.test_date(now)
      expect(result.class).to eq(Time)
      expect(result).to eq(Time.parse((now + 1).strftime("%c")))
    end
  
    it "should process nil date parameter as NULL" do
      expect(plsql.test_date(nil)).to be_nil
    end

  end

  describe "Function with timestamp parameters" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should process timestamp parameters" do
      # now = Time.now
      now = Time.local(2008,8,12,14,28,0)
      expect(plsql.test_timestamp(now)).to eq(now + 60*60*24)
    end

  end

  describe "Procedure with output parameters" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should return hash with output parameters" do
      expect(plsql.test_copy("abc", nil, nil)).to eq({ :p_to => "abc", :p_to_double => "abcabc" })
    end

    it "should return hash with output parameters when called with named parameters" do
      expect(plsql.test_copy(:p_from => "abc", :p_to => nil, :p_to_double => nil)).to eq({ :p_to => "abc", :p_to_double => "abcabc" })
    end

    it "should substitute output parameters with nil if they are not specified" do
      expect(plsql.test_copy("abc")).to eq({ :p_to => "abc", :p_to_double => "abcabc" })
    end

    it "should substitute named output parameters with nil if they are not specified" do
      expect(plsql.test_copy(:p_from => "abc")).to eq({ :p_to => "abc", :p_to_double => "abcabc" })
    end

  end

  describe "Package with procedures with same name but different argument lists" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
          FUNCTION test_function ( p_string VARCHAR2, p_string2 VARCHAR2 DEFAULT ' ')
            RETURN VARCHAR2;
          FUNCTION test_function ( p_number NUMBER, p_number2 NUMBER DEFAULT 1 )
            RETURN NUMBER;
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
          FUNCTION test_function ( p_string VARCHAR2, p_string2 VARCHAR2)
            RETURN VARCHAR2
          IS
          BEGIN
            RETURN p_string||p_string2;
          END;
          FUNCTION test_function ( p_number NUMBER, p_number2 NUMBER)
            RETURN NUMBER
          IS
          BEGIN
            RETURN p_number + p_number2;
          END;
        END;
      SQL

    end
  
    after(:all) do
      plsql.execute "DROP PACKAGE test_package2"
      plsql.logoff
    end
    
    it "should find existing package" do
      expect(PLSQL::Package.find(plsql, :test_package2)).not_to be_nil
    end

    it "should identify overloaded procedure definition" do
      @procedure = PLSQL::Procedure.find(plsql, :test_procedure, "TEST_PACKAGE2")
      expect(@procedure).not_to be_nil
      expect(@procedure).to be_overloaded
    end

    it "should identify non-overloaded procedure definition" do
      @procedure = PLSQL::Procedure.find(plsql, :test_procedure2, "TEST_PACKAGE2")
      expect(@procedure).not_to be_nil
      expect(@procedure).not_to be_overloaded
    end

    it "should execute correct procedures based on number of arguments and return correct value" do
      expect(plsql.test_package2.test_procedure('xxx')).to eq('XXX')
      expect(plsql.test_package2.test_procedure('xxx', nil)).to eq({:p_result => 'XXX'})
    end

    it "should execute correct procedures based on number of named arguments and return correct value" do
      expect(plsql.test_package2.test_procedure(:p_string => 'xxx')).to eq('XXX')
      expect(plsql.test_package2.test_procedure(:p_string => 'xxx', :p_result => nil)).to eq({:p_result => 'XXX'})
    end

    it "should raise exception if procedure cannot be found based on number of arguments" do
      expect { plsql.test_package2.test_procedure() }.to raise_error(/wrong number or types of arguments/i)
    end
  
    it "should find procedure based on types of arguments" do
      expect(plsql.test_package2.test_procedure(111, nil)).to eq({:p_result => '111'})
    end

    it "should find procedure based on names of named arguments" do
      expect(plsql.test_package2.test_procedure(:p_number => 111, :p_result => nil)).to eq({:p_result => '111'})
    end

    it "should find matching procedure based on partial list of named arguments" do
      expect(plsql.test_package2.test_function(:p_string => 'xxx')).to eq('xxx ')
      expect(plsql.test_package2.test_function(:p_number => 1)).to eq(2)
    end

  end

  describe "Function with output parameters" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should return array with return value and hash of output parameters" do
      expect(plsql.test_copy_function("abc", nil, nil)).to eq([3, { :p_to => "abc", :p_to_double => "abcabc" }])
    end

    it "should return array with return value and hash of output parameters when called with named parameters" do
      expect(plsql.test_copy_function(:p_from => "abc", :p_to => nil, :p_to_double => nil)).to eq(
        [3, { :p_to => "abc", :p_to_double => "abcabc" }]
      )
    end

    it "should substitute output parameters with nil if they are not specified" do
      expect(plsql.test_copy_function("abc")).to eq([3, { :p_to => "abc", :p_to_double => "abcabc" }])
    end

  end

  describe "Function or procedure without parameters" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end

    it "should find function" do
      expect(PLSQL::Procedure.find(plsql, :test_no_params)).not_to be_nil
    end

    it "should return function value" do
      expect(plsql.test_no_params).to eq("dummy")
    end

    it "should find procedure" do
      expect(PLSQL::Procedure.find(plsql, :test_proc_no_params)).not_to be_nil
    end

    it "should execute procedure" do
      expect(plsql.test_proc_no_params).to be_nil
    end

  end

  describe "Function with CLOB parameter and return value" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_clob
          ( p_clob CLOB )
          RETURN CLOB
        IS
        BEGIN
          RETURN p_clob;
        END test_clob;
      SQL
      plsql.execute "CREATE TABLE test_clob_table (clob_col CLOB)"
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_clob_insert
          ( p_clob CLOB )
          RETURN CLOB
        IS
          CURSOR clob_cur IS
          SELECT clob_col
          FROM test_clob_table;
          
          v_dummy  CLOB;
        BEGIN
          DELETE FROM test_clob_table;
          INSERT INTO test_clob_table (clob_col) VALUES (p_clob);

          OPEN clob_cur;
          FETCH clob_cur INTO v_dummy;
          CLOSE clob_cur;
          
          RETURN v_dummy;
        END test_clob_insert;
      SQL
    end
  
    after(:all) do
      plsql.execute "DROP FUNCTION test_clob"
      plsql.execute "DROP FUNCTION test_clob_insert"
      plsql.execute "DROP TABLE test_clob_table"
      plsql.logoff
    end
  
    it "should find existing procedure" do
      expect(PLSQL::Procedure.find(plsql, :test_clob)).not_to be_nil
    end

    it "should execute function and return correct value" do
      large_text = 'ābčdēfghij' * 10_000
      expect(plsql.test_clob(large_text)).to eq(large_text)
    end

    unless defined?(JRUBY_VERSION)

      it "should execute function with empty string and return nil (oci8 cannot pass empty CLOB parameter)" do
        text = ''
        expect(plsql.test_clob(text)).to be_nil
      end

      it "should execute function which inserts the CLOB parameter into a table with empty string and return nil" do
        text = ''
        expect(plsql.test_clob_insert(text)).to be_nil
      end
    
    else

      it "should execute function with empty string and return empty string" do
        text = ''
        expect(plsql.test_clob(text)).to eq(text)
      end
    
    end

    it "should execute function with nil and return nil" do
      expect(plsql.test_clob(nil)).to be_nil
    end

    it "should execute function which inserts the CLOB parameter into a table with nil and return nil" do
      expect(plsql.test_clob_insert(nil)).to be_nil
    end
    
  end

  describe "Procedrue with CLOB parameter and return value" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should find existing procedure" do
      expect(PLSQL::Procedure.find(plsql, :test_clob_proc)).not_to be_nil
    end

    it "should execute function and return correct value" do
      large_text = 'ābčdēfghij' * 10_000
      expect(plsql.test_clob_proc(large_text)[:p_return]).to eq(large_text)
    end
  end

  describe "Procedrue with BLOB parameter and return value" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end
  
    it "should find existing procedure" do
      expect(PLSQL::Procedure.find(plsql, :test_blob_proc)).not_to be_nil
    end

    it "should execute function and return correct value" do
      large_binary = '\000\001\002\003\004\005\006\007\010\011' * 10_000
      expect(plsql.test_blob_proc(large_binary)[:p_return]).to eq(large_binary)
    end
  end

  describe "Function with record parameter" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
        CREATE OR REPLACE PACKAGE test_record IS
          TYPE t_employee IS RECORD(
            employee_id   NUMBER(15),
            first_name    VARCHAR2(50),
            last_name     VARCHAR2(50),
            hire_date     DATE
          );

          TYPE t_candidate IS RECORD(
            candidate_id NUMBER(5),
            is_approved  BOOLEAN
          );

          TYPE table_of_records IS TABLE OF t_employee;

          FUNCTION test_full_name(p_employee t_employee)
            RETURN VARCHAR2;

          FUNCTION test_empty_records
            RETURN table_of_records;

          FUNCTION is_approved(p_candidate t_candidate)
            RETURN BOOLEAN;

          FUNCTION f_set_candidate_status(p_candidate t_candidate, p_status boolean)
            RETURN t_candidate;

          PROCEDURE p_set_candidate_status(p_candidate t_candidate, p_status boolean, p_result OUT t_candidate);
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_record IS
          FUNCTION test_full_name (p_employee t_employee)
            RETURN VARCHAR2
          IS
          BEGIN
            RETURN p_employee.first_name || ' ' || p_employee.last_name;
          END;

          FUNCTION test_empty_records
            RETURN table_of_records
          IS
            CURSOR employees_cur
            IS
              SELECT 
                null employee_id,
                null first_name,
                null last_name,
                null hire_date
              FROM dual
              WHERE 1 = 2;
            employees_tab table_of_records;
          BEGIN
            OPEN employees_cur;
            FETCH employees_cur BULK COLLECT INTO employees_tab;
            CLOSE employees_cur;
            RETURN employees_tab;
          END;

          FUNCTION is_approved(p_candidate t_candidate)
            RETURN BOOLEAN
          IS
          BEGIN
            RETURN p_candidate.is_approved;
          END;

          FUNCTION f_set_candidate_status(p_candidate t_candidate, p_status boolean)
            RETURN t_candidate
          IS
            result t_candidate := p_candidate;
          BEGIN
            result.is_approved := p_status;
            return result;
          END;

          PROCEDURE p_set_candidate_status(p_candidate t_candidate, p_status boolean, p_result OUT t_candidate)
          IS
          BEGIN
            p_result := p_candidate;
            p_result.is_approved := p_status;
          END;
        END;
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
        CREATE OR REPLACE FUNCTION test_employee_record2 (p_employee test_employees%ROWTYPE, x_employee IN OUT test_employees%ROWTYPE)
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
      plsql.execute "DROP PACKAGE test_record"
      plsql.execute "DROP FUNCTION test_employee_record"
      plsql.execute "DROP FUNCTION test_employee_record2"
      plsql.execute "DROP TABLE test_employees"
      plsql.logoff
    end

    it "should find existing function" do
      expect(PLSQL::Procedure.find(plsql, :test_full_name)).not_to be_nil
    end

    it "should execute function with named parameter and return correct value" do
      expect(plsql.test_full_name(:p_employee => @p_employee)).to eq('First Last')
    end

    it "should execute function with sequential parameter and return correct value" do
      expect(plsql.test_full_name(@p_employee)).to eq('First Last')
    end

    it "should execute function with Hash parameter using strings as keys" do
      expect(plsql.test_full_name(@p_employee2)).to eq('Second Last')
    end

    it "should raise error if wrong field name is passed for record parameter" do
      expect do
        expect(plsql.test_full_name(@p_employee.merge :xxx => 'xxx')).to eq('Second Last')
      end.to raise_error(ArgumentError)
    end
    
    it "should return empty table of records" do
      expect(plsql.test_record.test_empty_records()).to eq([])
    end

    it "should return record return value" do
      expect(plsql.test_employee_record(@p_employee)).to eq(@p_employee)
    end

    it "should return record return value and output record parameter value" do
      expect(plsql.test_employee_record2(@p_employee, @p_employee2)).to eq([@p_employee, {:x_employee => @p_employee}])
    end

    it "should execute package function with parameter with record type defined in package" do
      expect(plsql.test_record.test_full_name(@p_employee)).to eq('First Last')
    end

    context "functions with record parameters having boolean attributes" do
      def new_candidate(status)
        {:candidate_id => 1, :is_approved => status}
      end

      [true, false, nil].each do |status|
        it "should execute function with record having boolean attribute (#{status})" do
          expect(plsql.test_record.is_approved(new_candidate(status))).to eq status
        end

        it "procedure should return record with boolean attribute as output parameter (#{status})" do
          expect(plsql.test_record.p_set_candidate_status(new_candidate(nil), status)[:p_result]).to eq new_candidate(status)
        end

        it "function should return record with boolean attribute (#{status})" do
          expect(plsql.test_record.f_set_candidate_status(new_candidate(nil), status)).to eq new_candidate(status)
        end
      end
    end

  end

  describe "Function with boolean parameters" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end

    it "should accept true value and return true value" do
      expect(plsql.test_boolean(true)).to eq(true)
    end

    it "should accept false value and return false value" do
      expect(plsql.test_boolean(false)).to eq(false)
    end

    it "should accept nil value and return nil value" do
      expect(plsql.test_boolean(nil)).to be_nil
    end

    it "should accept true value and assign true value to output parameter" do
      expect(plsql.test_boolean2(true, nil)).to eq({:x_boolean => true})
    end

    it "should accept false value and assign false value to output parameter" do
      expect(plsql.test_boolean2(false, nil)).to eq({:x_boolean => false})
    end

    it "should accept nil value and assign nil value to output parameter" do
      expect(plsql.test_boolean2(nil, nil)).to eq({:x_boolean => nil})
    end

  end

  describe "Function with object type parameter" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end

    it "should find existing function" do
      expect(PLSQL::Procedure.find(plsql, :test_full_name)).not_to be_nil
    end

    it "should execute function with named parameter and return correct value" do
      expect(plsql.test_full_name(:p_employee => @p_employee)).to eq('First Last')
    end

    it "should execute function with sequential parameter and return correct value" do
      expect(plsql.test_full_name(@p_employee)).to eq('First Last')
    end

    it "should raise error if wrong field name is passed for record parameter" do
      expect do
        plsql.test_full_name(@p_employee.merge :xxx => 'xxx')
      end.to raise_error(ArgumentError)
    end

    it "should return object type return value" do
      expect(plsql.test_employee_object(@p_employee)).to eq(@p_employee)
    end

    it "should return object type return value and output object type parameter value" do
      expect(plsql.test_employee_object2(@p_employee, nil)).to eq([@p_employee, {:x_employee => @p_employee}])
    end

    it "should accept NULL as input parameter" do
      expect(plsql.test_employee_object(nil)).to eq(nil)
    end

  end


  describe "Function with table parameter" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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

      # Type definition inside package
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_collections IS
          TYPE t_numbers IS TABLE OF NUMBER(15);
          FUNCTION test_sum (p_numbers IN t_numbers)
            RETURN NUMBER;
          FUNCTION test_function_failure (p_numbers IN t_numbers, p_force_failure IN VARCHAR2 DEFAULT 'N')
            RETURN NUMBER;
          FUNCTION test_numbers (p_numbers IN t_numbers, x_numbers OUT t_numbers)
            RETURN t_numbers;
          TYPE t_employee IS RECORD(
            employee_id   NUMBER(15),
            first_name    VARCHAR2(50),
            last_name     VARCHAR2(50),
            hire_date     DATE
          );
          TYPE t_employees IS TABLE OF t_employee;
          FUNCTION test_employees (p_employees IN OUT t_employees)
            RETURN t_employees;
          -- these types with tables in lower level are not yet supported
          TYPE t_employee2 IS RECORD(
            employee_id   NUMBER(15),
            first_name    VARCHAR2(50),
            last_name     VARCHAR2(50),
            hire_date     DATE,
            numbers       t_numbers
          );
          FUNCTION test_employee2 (p_employee IN OUT t_employee2)
            RETURN t_employee2;
          TYPE t_employees2 IS TABLE OF t_employee2;
          FUNCTION test_employees2 (p_employees IN OUT t_employees2)
            RETURN t_employees2;

          TYPE t_nstring IS RECORD(
            ch_10bytes    CHAR(10 BYTE),
            ch_10chars    CHAR(10 CHAR),
            nch_10chars   NCHAR(10 CHAR),
            str_10bytes   VARCHAR2(10 BYTE),
            str_10chars   VARCHAR2(10 CHAR),
            nstr_10chars  NVARCHAR2(10)
          );
          TYPE t_nstrings IS TABLE OF t_nstring;
          FUNCTION test_nstring (p_strings IN t_nstrings, p_out OUT t_nstrings)
            return NVARCHAR2;
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
          FUNCTION test_function_failure (p_numbers IN t_numbers, p_force_failure IN VARCHAR2 DEFAULT 'N')
          RETURN NUMBER
          IS
          BEGIN
            IF p_force_failure = 'Y' THEN
              raise_application_error(-20000, 'Simulate business error to test clearing of temp table.');
            END IF;
            RETURN p_numbers.COUNT;
          END;
          FUNCTION test_numbers (p_numbers IN t_numbers, x_numbers OUT t_numbers)
          RETURN t_numbers
          IS
          BEGIN
            x_numbers := p_numbers;
            RETURN p_numbers;
          END;
          FUNCTION test_employees (p_employees IN OUT t_employees)
            RETURN t_employees
          IS
          BEGIN
            RETURN p_employees;
          END;
          FUNCTION test_employee2 (p_employee IN OUT t_employee2)
            RETURN t_employee2
          IS
          BEGIN
            RETURN p_employee;
          END;
          FUNCTION test_employees2 (p_employees IN OUT t_employees2)
            RETURN t_employees2
          IS
          BEGIN
            RETURN p_employees;
          END;
          FUNCTION test_nstring (p_strings IN t_nstrings, p_out OUT t_nstrings)
            return NVARCHAR2
          IS
            tmp1 NVARCHAR2(2000);
            x    pls_integer;
          BEGIN
            p_out := p_strings;
            IF p_strings.count=0 THEN
              RETURN N'';
            END IF;
            x := p_strings.first;
            WHILE x IS NOT NULL LOOP
              tmp1 := tmp1 || rtrim(p_strings(x).nch_10chars) || p_strings(x).nstr_10chars || ',';
              x := p_strings.next(x);
            END LOOP;
            RETURN tmp1;
          END;
        END;
      SQL
      @employees = (1..10).map do |i|
        {
          :employee_id => i,
          :first_name => "First #{i}",
          :last_name => "Last #{i}",
          :hire_date => Time.local(2000,01,i),
        }
      end
      @nstrings = (1..5).map do |i|
        {
          :ch_10bytes => "Ch #{i}B     ",
          :ch_10chars => "Ch #{i}C     ",
          :nch_10chars => "NCh #{i}     ",
          :str_10bytes => "Str #{i}C",
          :str_10chars => "Str #{i}B",
          :nstr_10chars => "NStr #{i}",
        }
      end

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
      plsql.connection.drop_session_ruby_temporary_tables
      plsql.logoff
    end

    it "should find existing function" do
      expect(PLSQL::Procedure.find(plsql, :test_sum)).not_to be_nil
    end

    it "should execute function with number array parameter" do
      expect(plsql.test_sum([1,2,3,4])).to eq(10)
    end
    
    it "should return number array return value" do
      expect(plsql.test_increment([1,2,3,4], 1)).to eq([2,3,4,5])
    end

    it "should execute function with string array and return string array output parameter" do
      strings = ['1','2','3','4']
      expect(plsql.test_copy_strings(strings)).to eq([strings, {:x_strings => strings}])
    end

    it "should execute function with table of numbers type (defined inside package) parameter" do
      expect(plsql.test_collections.test_sum([1,2,3,4])).to eq(10)
    end

    it "should clear temporary tables after executing function with table of numbers type even if an error occurs in the package" do
      # this should work fine
      expect(plsql.test_collections.test_function_failure([1,2,3,4], 'N')).to eq(4)
      # we will force a package error here to see if things get cleaned up before the next call
      expect { plsql.test_collections.test_function_failure([1,2,3,4], 'Y') }.to raise_error(/Simulate business error to test clearing of temp table/)
      # after the error in the first call temporary tables should be cleared
      expect(plsql.test_collections.test_function_failure([5,6,7], 'N')).to eq(3)
    end

    it "should return table of numbers type (defined inside package)" do
      expect(plsql.test_collections.test_numbers([1,2,3,4])).to eq([[1,2,3,4], {:x_numbers => [1,2,3,4]}])
    end

    it "should clear temporary tables after executing function with table of numbers type (defined inside package) parameter" do
      expect(plsql.test_collections.test_numbers([1,2,3,4])).to eq([[1,2,3,4], {:x_numbers => [1,2,3,4]}])
      # after first call temporary tables should be cleared
      expect(plsql.test_collections.test_numbers([1,2,3,4])).to eq([[1,2,3,4], {:x_numbers => [1,2,3,4]}])
    end

    it "should clear temporary tables when autocommit with table of numbers type (defined inside package) parameter" do
      old_autocommit = plsql.connection.autocommit?
      plsql.connection.autocommit = true
      numbers_array = (1..400).to_a
      expect(plsql.test_collections.test_numbers(numbers_array)).to eq([numbers_array, {:x_numbers => numbers_array}])
      # after first call temporary tables should be cleared
      expect(plsql.test_collections.test_numbers(numbers_array)).to eq([numbers_array, {:x_numbers => numbers_array}])
      plsql.connection.autocommit = old_autocommit
    end

    it "should execute function with table of records type (defined inside package) parameter" do
      expect(plsql.test_collections.test_employees(@employees)).to eq([@employees, {:p_employees => @employees}])
    end

    it "should execute function with table of records type (defined inside package and includes NVARCHAR columns) parameter" do
      expect(plsql.test_collections.test_nstring(@nstrings)).to eq([(1..5).map{|i| "NCh #{i}NStr #{i},"}.join, {:p_out => @nstrings}])
    end

    it "should execute function with object array and return object array output parameter" do
      phones = [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      expect(plsql.test_copy_objects(phones)).to eq([phones, {:x_phones => phones}])
    end

    it "should execute function with empty object array" do
      phones = []
      expect(plsql.test_copy_objects(phones)).to eq([phones, {:x_phones => phones}])
    end

    it "should raise error with record parameter that has table as element" do
      expect {
        expect(plsql.test_collections.test_employee2(@employees[0])).to eq([@employees[0], {:p_employee => @employees[0]}])
      }.to raise_error(ArgumentError, /TEST_COLLECTIONS\.T_NUMBERS definition inside package is not supported/)
    end

    it "should raise error with table of records parameter when record has table as element" do
      expect {
        expect(plsql.test_collections.test_employees2(@employees)).to eq([@employees, {:p_employees => @employees}])
      }.to raise_error(ArgumentError, /TEST_COLLECTIONS\.T_NUMBERS definition inside package is not supported/)
    end

  end

  describe "Function with table indexed by bynary integer parameter" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE TABLE test_employees (
          employee_id   NUMBER(15),
          first_name    VARCHAR2(50),
          last_name     VARCHAR2(50),
          hire_date     DATE
        )
      SQL
      # Type definition inside package
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_collections IS
          TYPE t_numbers IS TABLE OF NUMBER(15)
            INDEX BY BINARY_INTEGER;
          FUNCTION test_sum (p_numbers IN t_numbers)
            RETURN NUMBER;
          FUNCTION test_function_failure (p_numbers IN t_numbers, p_force_failure IN VARCHAR2 DEFAULT 'N')
            RETURN NUMBER;
          FUNCTION test_numbers (p_numbers IN t_numbers, x_numbers OUT t_numbers)
            RETURN t_numbers;
          TYPE t_employee IS RECORD(
            employee_id   NUMBER(15),
            first_name    VARCHAR2(50),
            last_name     VARCHAR2(50),
            hire_date     DATE
          );
          TYPE t_employees IS TABLE OF t_employee
            INDEX BY BINARY_INTEGER;
          FUNCTION test_employees (p_employees IN OUT t_employees)
            RETURN t_employees;
          PROCEDURE test_employees_prc (p_employees IN OUT t_employees);
          PROCEDURE insert_employees(p_employees IN t_employees);
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_collections IS
          FUNCTION test_sum (p_numbers IN t_numbers)
          RETURN NUMBER
          IS
            l_sum   NUMBER(15) := 0;
            i BINARY_INTEGER;
          BEGIN
            IF p_numbers.COUNT > 0 THEN
              i := p_numbers.FIRST;
              LOOP
                EXIT WHEN i IS NULL;
                l_sum := l_sum + p_numbers(i);
                i := p_numbers.NEXT(i);
              END LOOP;
              RETURN l_sum;
            ELSE
              RETURN NULL;
            END IF;
          END;
          FUNCTION test_function_failure (p_numbers IN t_numbers, p_force_failure IN VARCHAR2 DEFAULT 'N')
          RETURN NUMBER
          IS
            l_sum   NUMBER(15) := 0;
            i BINARY_INTEGER;
          BEGIN
            IF p_force_failure = 'Y' THEN
              raise_application_error(-20000, 'Simulate business error to test clearing of temp table.');
            END IF;
            IF p_numbers.COUNT > 0 THEN
              i := p_numbers.FIRST;
              LOOP
                EXIT WHEN i IS NULL;
                l_sum := l_sum + p_numbers(i);
                i := p_numbers.NEXT(i);
              END LOOP;
              RETURN l_sum;
            ELSE
              RETURN NULL;
            END IF;
          END;
          FUNCTION test_numbers (p_numbers IN t_numbers, x_numbers OUT t_numbers)
          RETURN t_numbers
          IS
          BEGIN
            x_numbers := p_numbers;
            RETURN p_numbers;
          END;
          FUNCTION test_employees (p_employees IN OUT t_employees)
            RETURN t_employees
          IS
          BEGIN
            RETURN p_employees;
          END;
          PROCEDURE test_employees_prc (p_employees IN OUT t_employees)
          IS
          BEGIN
            NULL;
          END;
          PROCEDURE insert_employees(p_employees IN t_employees) IS
          BEGIN
            FORALL i IN p_employees.FIRST..p_employees.LAST
              INSERT INTO test_employees VALUES p_employees(i);
          END;
        END;
      SQL
      # test with negative PL/SQL table indexes
      @numbers = Hash[*(1..4).map{|i|[-i,i]}.flatten]
      @numbers2 = Hash[*(5..7).map{|i|[-i,i]}.flatten]
      # test with reversed PL/SQL table indexes
      @employees = Hash[*(1..10).map do |i|
          [11-i, {
              :employee_id => i,
              :first_name => "First #{i}",
              :last_name => "Last #{i}",
              :hire_date => Time.local(2000,01,i)
            }]
        end.flatten]
    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_collections"
      plsql.execute "DROP TABLE test_employees"
      plsql.connection.drop_session_ruby_temporary_tables
      plsql.logoff
    end

    it "should clear temporary tables after executing function with index-by table of numbers type even if an error occurs in the package" do
      # this should work fine
      expect(plsql.test_collections.test_function_failure(@numbers, 'N')).to eq(10)
      # we will force a package error here to see if things get cleaned up before the next call
      expect { plsql.test_collections.test_function_failure(@numbers, 'Y') }.to raise_error(/Simulate business error to test clearing of temp table/)
      # after the error in the first call temporary tables should be cleared
      expect(plsql.test_collections.test_function_failure(@numbers2, 'N')).to eq(18)
    end

    it "should execute function with index-by table of numbers type (defined inside package) parameter" do
      expect(plsql.test_collections.test_sum(@numbers)).to eq(10)
    end

    it "should return index-by table of numbers type (defined inside package)" do
      expect(plsql.test_collections.test_numbers(@numbers)).to eq([@numbers, {:x_numbers => @numbers}])
    end

    it "should clear temporary tables when autocommit with index-by table of numbers type (defined inside package) parameter" do
      old_autocommit = plsql.connection.autocommit?
      plsql.connection.autocommit = true
      numbers_hash = Hash[*(1..400).map{|i|[i,i]}.flatten]
      expect(plsql.test_collections.test_numbers(numbers_hash)).to eq([numbers_hash, {:x_numbers => numbers_hash}])
      # after first call temporary tables should be cleared
      expect(plsql.test_collections.test_numbers(numbers_hash)).to eq([numbers_hash, {:x_numbers => numbers_hash}])
      plsql.connection.autocommit = old_autocommit
    end

    it "should execute function with index-by table of records type (defined inside package) parameter" do
      expect(plsql.test_collections.test_employees(@employees)).to eq([@employees, {:p_employees => @employees}])
    end

    it "should execute procedure with index-by table of records type (defined inside package) parameter" do
      expect(plsql.test_collections.test_employees_prc(@employees)).to eq({:p_employees => @employees})
    end

    it "should create temporary tables in autonomous transaction" do
      old_autocommit = plsql.connection.autocommit?
      plsql.connection.autocommit = false
      plsql.test_employees.insert @employees[1]
      # procedure call should not commit initial insert
      plsql.test_collections.insert_employees(2=>@employees[2], 3=>@employees[3])
      plsql.rollback
      expect(plsql.test_employees.all).to be_empty
      plsql.connection.autocommit = old_autocommit
    end

    describe "using Oracle 9.2" do
      before do
        # simulate Oracle 9.2 connection
        plsql(:oracle_9).connection = get_connection
        allow(plsql(:oracle_9).connection).to receive(:database_version).and_return([9, 2, 0, 0])
      end

      after do
        plsql(:oracle_9).logoff
      end

      it "should create temporary tables when using Oracle 9.2" do
        expect(plsql(:oracle_9).test_collections.test_numbers(@numbers)).to eq([@numbers, {:x_numbers => @numbers}])
      end

    end

  end


  describe "Function with VARRAY parameter" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end

    it "should find existing function" do
      expect(PLSQL::Procedure.find(plsql, :test_sum)).not_to be_nil
    end

    it "should execute function with number array parameter" do
      expect(plsql.test_sum([1,2,3,4])).to eq(10)
    end

    it "should return number array return value" do
      expect(plsql.test_increment([1,2,3,4], 1)).to eq([2,3,4,5])
    end

    it "should execute function with string array and return string array output parameter" do
      strings = ['1','2','3','4']
      expect(plsql.test_copy_strings(strings)).to eq([strings, {:x_strings => strings}])
    end

    it "should execute function with object array and return object array output parameter" do
      phones = [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      expect(plsql.test_copy_objects(phones)).to eq([phones, {:x_phones => phones}])
    end

    it "should execute function with empty object array" do
      phones = []
      expect(plsql.test_copy_objects(phones)).to eq([phones, {:x_phones => phones}])
    end

  end

  describe "Function in package with VARRAY parameter" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE TYPE t_phone AS OBJECT (
          type            VARCHAR2(10),
          phone_number    VARCHAR2(50)
        )
      SQL

      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_collections IS
          TYPE t_numbers_array IS VARRAY(100) OF NUMBER(15);
          TYPE t_strings_array IS VARRAY(100) OF VARCHAR2(4000);
          TYPE t_phones_array IS ARRAY(100) OF T_PHONE;
          FUNCTION test_sum (p_numbers IN t_numbers_array)
            RETURN NUMBER;
          FUNCTION test_function_failure (p_numbers IN t_numbers_array, p_force_failure IN VARCHAR2 DEFAULT 'N')
            RETURN NUMBER;
          FUNCTION test_increment(p_numbers IN t_numbers_array, p_increment_by IN NUMBER DEFAULT 1)
            RETURN t_numbers_array;
          FUNCTION test_copy_strings(p_strings IN t_strings_array, x_strings OUT t_strings_array)
            RETURN t_strings_array;
          FUNCTION test_copy_objects(p_phones IN t_phones_array, x_phones OUT t_phones_array)
            RETURN t_phones_array;
        END;
      SQL

      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_collections IS
          FUNCTION test_sum (p_numbers IN t_numbers_array)
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

          FUNCTION test_function_failure (p_numbers IN t_numbers_array, p_force_failure IN VARCHAR2 DEFAULT 'N')
            RETURN NUMBER
          IS
            l_sum   NUMBER(15) := 0;
          BEGIN
            IF p_force_failure = 'Y' THEN
              raise_application_error(-20000, 'Simulate business error to test clearing of temp table.');
            END IF;
            IF p_numbers.COUNT > 0 THEN
              FOR i IN p_numbers.FIRST..p_numbers.LAST LOOP
                l_sum := l_sum + p_numbers(i);
              END LOOP;
              RETURN l_sum;
            ELSE
              RETURN NULL;
            END IF;
          END;

          FUNCTION test_increment(p_numbers IN t_numbers_array, p_increment_by IN NUMBER DEFAULT 1)
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

          FUNCTION test_copy_strings(p_strings IN t_strings_array, x_strings OUT t_strings_array)
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

          FUNCTION test_copy_objects(p_phones IN t_phones_array, x_phones OUT t_phones_array)
            RETURN t_phones_array
          IS
          BEGIN
            x_phones := p_phones;
            RETURN x_phones;
          END;
        END;
      SQL
    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_collections"
      plsql.execute "DROP TYPE t_phone" rescue nil
      plsql.logoff
    end

    it "should execute function with number array parameter" do
      expect(plsql.test_collections.test_sum([1,2,3,4])).to eq(10)
    end

    it "should clear temporary tables after executing function with varray of numbers type even if an error occurs in the package" do
      # this should work fine
      expect(plsql.test_collections.test_function_failure([1,2,3,4], 'N')).to eq(10)
      # we will force a package error here to see if things get cleaned up before the next call
      expect { plsql.test_collections.test_function_failure([5,6,7], 'Y') }.to raise_error(/Simulate business error to test clearing of temp table/)
      # after the error in the first call temporary tables should be cleared
      expect(plsql.test_collections.test_function_failure([3,4,5,6], 'N')).to eq(18)
    end

    it "should return number array return value" do
      expect(plsql.test_collections.test_increment([1,2,3,4], 1)).to eq([2,3,4,5])
    end

    it "should execute function with string array and return string array output parameter" do
      strings = ['1','2','3','4']
      expect(plsql.test_collections.test_copy_strings(strings)).to eq([strings, {:x_strings => strings}])
    end

    it "should execute function with object array and return object array output parameter" do
      phones = [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      expect(plsql.test_collections.test_copy_objects(phones)).to eq([phones, {:x_phones => phones}])
    end

    # This test fails without wcmatthysen's "Procedure-call Fix." pull request.
    # it "should execute function with empty object array" do
    #   phones = []
    #   plsql.test_collections.test_copy_objects(phones).should == [phones, {:x_phones => phones}]
    # end

  end

  describe "Function with cursor return value or parameter" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
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
      plsql.logoff
    end

    it "should find existing function" do
      expect(PLSQL::Procedure.find(plsql, :test_cursor)).not_to be_nil
    end

    it "should return cursor and fetch first row" do
      expect(plsql.test_cursor do |cursor|
        expect(cursor.fetch).to eq(@fields.map{|f| @employees[0][f]})
      end).to be_nil
    end

    it "should close all returned cursors after block is executed" do
      cursor2 = nil
      expect(plsql.test_cursor do |cursor|
        cursor2 = cursor
      end).to be_nil
      expect { cursor2.fetch }.to raise_error
    end

    it "should not raise error if cursor is closed inside block" do
      expect do
        plsql.test_cursor do |cursor|
          cursor.close
        end
      end.not_to raise_error
    end

    it "should fetch hash from returned cursor" do
      plsql.test_cursor do |cursor|
        expect(cursor.fetch_hash).to eq(@employees[0])
      end
    end

    it "should fetch all rows from returned cursor" do
      plsql.test_cursor do |cursor|
        expect(cursor.fetch_all).to eq(@employees.map{|e| @fields.map{|f| e[f]}})
      end
    end

    it "should fetch all rows as hash from returned cursor" do
      plsql.test_cursor do |cursor|
        expect(cursor.fetch_hash_all).to eq(@employees)
      end
    end

    it "should get field names from returned cursor" do
      plsql.test_cursor do |cursor|
        expect(cursor.fields).to eq(@fields)
      end
    end

    it "should return output parameter with cursor and fetch first row" do
      expect(plsql.test_cursor_out do |result|
        expect(result[:x_cursor].fetch).to eq(@fields.map{|f| @employees[0][f]})
      end).to be_nil
    end

    it "should return output parameter with cursor and fetch all rows as hash" do
      expect(plsql.test_cursor_out do |result|
        expect(result[:x_cursor].fetch_hash_all).to eq(@employees)
      end).to be_nil
    end

    it "should execute function with cursor parameter and return record" do
      skip "not possible from JDBC" if defined?(JRUBY_VERSION)
      plsql.test_cursor do |cursor|
        expect(plsql.test_cursor_fetch(cursor)).to eq(@employees[0])
      end
    end

  end

  describe "Function with typed ref cursor return value" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute "DROP TABLE typed_ref_cursor_table" rescue nil

      plsql.execute <<-SQL
        CREATE TABLE typed_ref_cursor_table
        ( col1 VARCHAR2(10), col2 NUMBER )
      SQL

      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE typed_ref_cursor_test IS
          TYPE test_rec IS RECORD ( col1 VARCHAR2(10), col2 NUMBER ) ;
          TYPE test_rec_ref IS REF CURSOR RETURN test_rec ;

          function get_all RETURN test_rec_ref ;
        END typed_ref_cursor_test ;
      SQL

      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY typed_ref_cursor_test IS
          FUNCTION get_all RETURN test_rec_ref IS
            rc test_rec_ref ;
          BEGIN
            OPEN rc FOR SELECT * FROM typed_ref_cursor_table ;
            RETURN rc ;
          END get_all ;
        END typed_ref_cursor_test ;
      SQL

      @fields = [:col1, :col2 ]
      @rows = (1..3).map{|i| ["row #{i}", i]}
      plsql.typed_ref_cursor_table.insert_values *@rows
      plsql.commit

    end

    after(:all) do
      plsql.execute "DROP PACKAGE typed_ref_cursor_test"
      plsql.execute "DROP TABLE typed_ref_cursor_table"
      plsql.logoff
    end

    it "should return cursor and fetch first row" do
      expect(plsql.typed_ref_cursor_test.get_all do |cursor|
        expect(cursor.fetch).to eq(@rows[0])
      end).to be_nil
    end

    it "should fetch hash from returned cursor" do
      plsql.typed_ref_cursor_test.get_all do |cursor|
        expect(cursor.fetch_hash).to eq(Hash[*@fields.zip(@rows[0]).flatten])
      end
    end

    it "should fetch all rows from returned cursor" do
      plsql.typed_ref_cursor_test.get_all do |cursor|
        expect(cursor.fetch_all).to eq(@rows)
      end
    end

  end

end

describe "Synonyms /" do
  before(:all) do
    plsql.connect! CONNECTION_PARAMS
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
      expect(PLSQL::Procedure.find(plsql, :test_synonym)).not_to be_nil
    end

    it "should execute function using synonym and return correct value" do
      expect(plsql.test_synonym('xxx')).to eq('XXX')
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
      expect(PLSQL::Procedure.find(plsql, :ora_login_user)).not_to be_nil
    end

    it "should execute function using public synonym and return correct value" do
      expect(plsql.ora_login_user).to eq('HR')
    end

    it "should not find public synonym if schema prefix is used" do
      expect { plsql.hr.ora_login_user }.to raise_error(ArgumentError)
    end

    it "should find private synonym before public synonym" do
      # should reconnect to force clearing of procedure cache
      plsql.connection = get_connection
      plsql.execute "DROP SYNONYM ora_login_user" rescue nil
      plsql.execute "CREATE SYNONYM ora_login_user FOR hr.test_ora_login_user"
      expect(plsql.ora_login_user).to eq('XXX')
      plsql.execute "DROP SYNONYM ora_login_user"
      plsql.connection = get_connection
      expect(plsql.ora_login_user).to eq('HR')
    end

  end

  describe "invalid objects" do
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_invalid_function(p_dummy VARCHAR2) RETURN VARCHAR2 IS
          l_dummy invalid_table.invalid_column%TYPE;
        BEGIN
          RETURN p_dummy;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_invalid_package IS
          FUNCTION test_invalid_function(p_dummy VARCHAR2) RETURN VARCHAR2;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_invalid_package IS
          FUNCTION test_invalid_function(p_dummy VARCHAR2) RETURN VARCHAR2 IS
            l_dummy1 invalid_table.invalid_column%TYPE;
            l_dummy2 invalid_table.invalid_column%TYPE;
          BEGIN
            RETURN p_dummy;
          END;
        END;
      SQL
    end

    after(:all) do
      plsql.execute "DROP FUNCTION test_invalid_function"
      plsql.execute "DROP PACKAGE test_invalid_package"
    end

    it "should raise error when invalid function is called" do
      expect {
        plsql.test_invalid_function('test')
      }.to raise_error(ArgumentError, /is not in valid status/)
    end

    it "should raise error when function from invalid package body is called" do
      expect {
        plsql.test_invalid_package.test_invalid_function('test')
      }.to raise_error(ArgumentError, /body is not in valid status/)
    end
  end

end

describe "SYS.STANDARD procedures /" do

  before(:all) do
    plsql.connect! CONNECTION_PARAMS
  end

  after(:all) do
    plsql.logoff
  end

  it "should execute function from SYS.STANDARD package" do
    expect(plsql.upper('abc')).to eq('ABC')
  end

  it "should find function overload based on types of sequential arguments" do
    expect(plsql.nvl(1, 2)).to eq(1)
    expect(plsql.nvl(nil, 2)).to eq(2)
    expect(plsql.nvl(1.1, 2.2)).to eq(1.1)
    expect(plsql.nvl(nil, 2.2)).to eq(2.2)
    expect(plsql.nvl(BigDecimal('1.1'), BigDecimal('2.2'))).to eq(BigDecimal('1.1'))
    expect(plsql.nvl(nil, BigDecimal('2.2'))).to eq(BigDecimal('2.2'))
    expect(plsql.nvl('a', 'b')).to eq('a')
    expect(plsql.nvl(nil, 'b')).to eq('b')
    expect(plsql.nvl(Date.new(2010,1,13), Date.new(2010,1,19))).to eq(Time.local(2010,1,13))
    expect(plsql.nvl(nil, Date.new(2010,1,19))).to eq(Time.local(2010,1,19))
    expect(plsql.nvl(Time.local(2010,1,13), Time.local(2010,1,19))).to eq(Time.local(2010,1,13))
    expect(plsql.nvl(nil, Time.local(2010,1,19))).to eq(Time.local(2010,1,19))
    expect(plsql.nvl(true, false)).to eq(true)
    expect(plsql.nvl(nil, false)).to eq(false)
  end

end

describe "PLS_INTEGER/SIMPLE_INTEGER should be nullable" do

  before(:all) do
    plsql.connect! CONNECTION_PARAMS
    plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_pls_f ( p_num PLS_INTEGER ) RETURN PLS_INTEGER IS
        BEGIN
          RETURN p_num;
        END;
    SQL
    plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_bin_f ( p_num BINARY_INTEGER ) RETURN BINARY_INTEGER IS
        BEGIN
          RETURN p_num;
        END;
    SQL
    plsql.execute <<-SQL
        CREATE OR REPLACE FUNCTION test_int_f ( p_num INTEGER ) RETURN INTEGER IS
        BEGIN
          RETURN p_num;
        END;
    SQL
    plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_pls_p ( p_num IN OUT PLS_INTEGER ) IS
        BEGIN
          NULL;
        END;
    SQL
    plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_bin_p ( p_num IN OUT BINARY_INTEGER ) IS
        BEGIN
          NULL;
        END;
    SQL
    plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_int_p ( p_num IN OUT INTEGER ) IS
        BEGIN
          NULL;
        END;
    SQL
    plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_flt_p ( p_num IN OUT BINARY_FLOAT ) IS
        BEGIN
          NULL;
        END;
    SQL
    plsql.execute <<-SQL
        CREATE OR REPLACE PROCEDURE test_dbl_p ( p_num IN OUT BINARY_DOUBLE ) IS
        BEGIN
          NULL;
        END;
    SQL
  end

  after(:all) do
    plsql.execute "DROP FUNCTION test_pls_f"
    plsql.execute "DROP FUNCTION test_bin_f"
    plsql.execute "DROP FUNCTION test_int_f"
    plsql.execute "DROP PROCEDURE test_pls_p"
    plsql.execute "DROP PROCEDURE test_int_p"
    plsql.execute "DROP PROCEDURE test_flt_p"
    plsql.execute "DROP PROCEDURE test_dbl_p"
    plsql.logoff
  end

  it 'should return null for a function call with NULL PLS_INTEGER param' do
    expect(plsql.test_pls_f(nil)).to be_nil
  end

  it 'should return null for a function call with NULL BINARY_INTEGER param' do
    expect(plsql.test_bin_f(nil)).to be_nil
  end

  it 'should return null for a function call with NULL INTEGER param' do
    expect(plsql.test_int_f(nil)).to be_nil
  end

  it 'should return null for a procedure call with NULL PLS_INTEGER param' do
    expect(plsql.test_pls_p(nil)[:p_num]).to be_nil
  end

  it 'should return null for a procedure call with NULL BINARY_INTEGER param' do
    expect(plsql.test_bin_p(nil)[:p_num]).to be_nil
  end

  it 'should return null for a procedure call with NULL INTEGER param' do
    expect(plsql.test_int_p(nil)[:p_num]).to be_nil
  end

  it 'should return null for a procedure call with NULL BINARY_FLOAT param' do
    expect(plsql.test_flt_p(nil)[:p_num]).to be_nil
  end

  it 'should return null for a procedure call with NULL BINARY_DOUBLE param' do
    expect(plsql.test_dbl_p(nil)[:p_num]).to be_nil
  end

end
