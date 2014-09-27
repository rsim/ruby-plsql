# encoding: utf-8

require 'spec_helper'

describe "Package variables /" do

  describe "String" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          varchar2_variable VARCHAR2(50);
          varchar2_variable2 VARCHAR2(50); -- some comment
          varchar2_default varchar2(50) := 'default' ;
          varchar2_default2 varchar2(50) DEFAULT 'default';
          varchar2_default3 varchar2(50) NOT NULL := 'default';
          varchar2_3_char VARCHAR2(3 CHAR);
          varchar2_3_byte VARCHAR2(3 BYTE);
          char_variable char(10) ;
          nvarchar2_variable NVARCHAR2(50);
          nchar_variable NCHAR(10);
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.logoff
    end

    it "should set and get VARCHAR2 variable" do
      plsql.test_package.varchar2_variable = 'abc'
      expect(plsql.test_package.varchar2_variable).to eq('abc')
    end

    it "should set and get VARCHAR2 variable with comment" do
      plsql.test_package.varchar2_variable2 = 'abc'
      expect(plsql.test_package.varchar2_variable2).to eq('abc')
    end

    it "should get VARCHAR2 variable default value" do
      expect(plsql.test_package.varchar2_default).to eq('default')
      expect(plsql.test_package.varchar2_default2).to eq('default')
      expect(plsql.test_package.varchar2_default3).to eq('default')
    end

    describe "with character or byte limit" do
      before(:each) do
        if !defined?(JRUBY_VERSION) && OCI8.properties.has_key?(:length_semantics)
          @original_length_semantics = OCI8.properties[:length_semantics]
          OCI8.properties[:length_semantics] = :char
        end
      end

      after(:each) do
        if !defined?(JRUBY_VERSION) && OCI8.properties.has_key?(:length_semantics)
          OCI8.properties[:length_semantics] = @original_length_semantics
        end
      end

      it "should set and get VARCHAR2(n CHAR) variable" do
        plsql.test_package.varchar2_3_char = 'āčē'
        expect(plsql.test_package.varchar2_3_char).to eq('āčē')
        expect { plsql.test_package.varchar2_3_char = 'aceg' }.to raise_error(/buffer too small/)
      end

      it "should set and get VARCHAR2(n BYTE) variable" do
        plsql.test_package.varchar2_3_byte = 'ace'
        expect(plsql.test_package.varchar2_3_byte).to eq('ace')
        expect { plsql.test_package.varchar2_3_byte = 'āce' }.to raise_error(/buffer too small/)
        expect { plsql.test_package.varchar2_3_byte = 'aceg' }.to raise_error(/buffer too small/)
      end

    end

    it "should set and get CHAR variable" do
      plsql.test_package.char_variable = 'abc'
      expect(plsql.test_package.char_variable).to eq('abc' + ' '*7)
    end

    it "should set and get NVARCHAR2 variable" do
      plsql.test_package.nvarchar2_variable = 'abc'
      expect(plsql.test_package.nvarchar2_variable).to eq('abc')
    end

    it "should set and get NCHAR variable" do
      plsql.test_package.nchar_variable = 'abc'
      expect(plsql.test_package.nchar_variable).to eq('abc' + ' '*7)
    end

  end

  describe "Numeric" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          integer_variable INTEGER;
          integer10_variable NUMBER(10);
          integer10_default NUMBER(10) := 1;
          number_variable NUMBER;
          number_with_scale NUMBER(15,2);
          pls_int_variable PLS_INTEGER;
          bin_int_variable BINARY_INTEGER;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.logoff
    end

    it "should set and get INTEGER variable" do
      plsql.test_package.integer_variable = 1
      expect(plsql.test_package.integer_variable).to be_a Fixnum
      expect(plsql.test_package.integer_variable).to eq(1)
    end

    it "should set and get integer variable with precision" do
      plsql.test_package.integer10_variable = 1
      expect(plsql.test_package.integer10_variable).to be_a Fixnum
      expect(plsql.test_package.integer10_variable).to eq(1)
    end

    it "should get integer variable default value" do
      expect(plsql.test_package.integer10_default).to eq(1)
    end

    it "should set and get PLS_INTEGER variable" do
      plsql.test_package.pls_int_variable = 1
      expect(plsql.test_package.pls_int_variable).to be_a Fixnum
      expect(plsql.test_package.pls_int_variable).to eq(1)
    end

    it "should set and get BINARY_INTEGER variable" do
      plsql.test_package.bin_int_variable = 1
      expect(plsql.test_package.bin_int_variable).to be_a Fixnum
      expect(plsql.test_package.bin_int_variable).to eq(1)
    end

    it "should set and get NUMBER variable" do
      plsql.test_package.number_variable = 123.456
      expect(plsql.test_package.number_variable).to be_a BigDecimal
      expect(plsql.test_package.number_variable).to eq(123.456)
    end

    it "should set and get NUMBER variable with scale" do
      plsql.test_package.number_with_scale = 123.456
      expect(plsql.test_package.number_with_scale).to eq(123.46) # rounding to two decimal digits
    end

  end

  describe "Date and Time" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          date_variable DATE;
          date_default DATE := TO_DATE('2009-12-21', 'YYYY-MM-DD');
          timestamp_variable TIMESTAMP;
          timestamptz_variable TIMESTAMP WITH TIME ZONE;
          timestampltz_variable TIMESTAMP WITH LOCAL TIME ZONE;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL
      @date = Time.local(2009,12,21)
      @timestamp = Time.local(2009,12,21,14,10,30,11)
    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.logoff
    end

    it "should set and get DATE variable" do
      plsql.test_package.date_variable = @date
      expect(plsql.test_package.date_variable).to be_a Time
      expect(plsql.test_package.date_variable).to eq(@date)
    end

    it "should get DATE variable default value" do
      expect(plsql.test_package.date_default).to eq(@date)
    end

    it "should set and get TIMESTAMP variable" do
      plsql.test_package.timestamp_variable = @timestamp
      expect(plsql.test_package.timestamp_variable).to be_a Time
      expect(plsql.test_package.timestamp_variable).to eq(@timestamp)
    end

    it "should set and get TIMESTAMP WITH TIME ZONE variable" do
      plsql.test_package.timestamptz_variable = @timestamp
      expect(plsql.test_package.timestamptz_variable).to be_a Time
      expect(plsql.test_package.timestamptz_variable).to eq(@timestamp)
    end

    it "should set and get TIMESTAMP WITH LOCAL TIME ZONE variable" do
      plsql.test_package.timestampltz_variable = @timestamp
      expect(plsql.test_package.timestampltz_variable).to be_a Time
      expect(plsql.test_package.timestampltz_variable).to eq(@timestamp)
    end

  end

  describe "LOB" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          clob_variable CLOB;
          clob_default CLOB := 'default';
          nclob_variable CLOB;
          blob_variable BLOB;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.logoff
    end

    it "should set and get CLOB variable" do
      plsql.test_package.clob_variable = 'abc'
      expect(plsql.test_package.clob_variable).to eq('abc')
    end

    it "should get CLOB variable default value" do
      expect(plsql.test_package.clob_default).to eq('default')
    end

    it "should set and get NCLOB variable" do
      plsql.test_package.nclob_variable = 'abc'
      expect(plsql.test_package.nclob_variable).to eq('abc')
    end

    it "should set and get BLOB variable" do
      plsql.test_package.blob_variable = "\000\001\003"
      expect(plsql.test_package.blob_variable).to eq("\000\001\003")
    end

  end

  describe "table column type" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE TABLE test_employees (
          employee_id NUMBER(15),
          first_name  VARCHAR2(50),
          last_name   VARCHAR2(50),
          hire_date   DATE
        )
      SQL

      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          employee_id test_employees.employee_id%TYPE;
          first_name  test_employees.first_name%TYPE;
          hire_date   test_employees.hire_date%TYPE;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.execute "DROP TABLE test_employees"
      plsql.logoff
    end

    it "should set and get NUMBER variable" do
      plsql.test_package.employee_id = 1
      expect(plsql.test_package.employee_id).to eq(1)
    end

    it "should set and get VARCHAR2 variable" do
      plsql.test_package.first_name = 'First'
      expect(plsql.test_package.first_name).to eq('First')
    end

    it "should set and get DATE variable" do
      today = Time.local(2009,12,22)
      plsql.test_package.hire_date = today
      expect(plsql.test_package.hire_date).to eq(today)
    end

  end

  describe "constants" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          integer_constant CONSTANT NUMBER(1) := 1;
          string_constant CONSTANT  VARCHAR2(10) := 'constant';
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.logoff
    end

    it "should get NUMBER constant" do
      expect(plsql.test_package.integer_constant).to eq(1)
    end

    it "should get VARCHAR2 constant" do
      expect(plsql.test_package.string_constant).to eq('constant')
    end

    it "should raise error when trying to set constant" do
      expect {
        plsql.test_package.integer_constant = 2
      }.to raise_error(/PLS-00363/)
    end

  end

  describe "object type" do
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
      @phones = [{:type => 'mobile', :phone_number => '123456'}, {:type => 'home', :phone_number => '654321'}]
      @employee = {
        :employee_id => 1,
        :first_name => 'First',
        :last_name => 'Last',
        :hire_date => Time.local(2000,01,31),
        :address => {:street => 'Main street 1', :city => 'Riga', :country => 'Latvia'},
        :phones => @phones
      }

      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          g_employee    t_employee;
          g_employee2   hr.t_employee;
          g_phones      t_phones;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.execute "DROP TYPE t_employee"
      plsql.execute "DROP TYPE t_address"
      plsql.execute "DROP TYPE t_phones"
      plsql.execute "DROP TYPE t_phone"
      plsql.logoff
    end

    it "should set and get object type variable" do
      plsql.test_package.g_employee = @employee
      expect(plsql.test_package.g_employee).to eq(@employee)
    end

    it "should set and get object type variable when schema prefix is used with type" do
      plsql.hr.test_package.g_employee2 = @employee
      expect(plsql.hr.test_package.g_employee2).to eq(@employee)
    end

    it "should set and get collection type variable" do
      plsql.test_package.g_phones = @phones
      expect(plsql.test_package.g_phones).to eq(@phones)
    end

  end

  describe "table row type" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE TABLE test_employees (
          employee_id NUMBER(15),
          first_name  VARCHAR2(50),
          last_name   VARCHAR2(50),
          hire_date   DATE
        )
      SQL
      @employee = {
        :employee_id => 1,
        :first_name => 'First',
        :last_name => 'Last',
        :hire_date => Time.local(2000,01,31)
      }

      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          g_employee test_employees%ROWTYPE;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.execute "DROP TABLE test_employees"
      plsql.logoff
    end

    it "should set and get table ROWTYPE variable" do
      plsql.test_package.g_employee = @employee
      expect(plsql.test_package.g_employee).to eq(@employee)
    end

  end

  describe "booleans" do
    before(:all) do
      plsql.connect! CONNECTION_PARAMS
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          boolean_variable BOOLEAN;
        END;
      SQL
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE BODY test_package IS
        END;
      SQL

    end

    after(:all) do
      plsql.execute "DROP PACKAGE test_package"
      plsql.logoff
    end

    it "should set and get BOOLEAN variable" do
      expect(plsql.test_package.boolean_variable).to be_nil
      plsql.test_package.boolean_variable = true
      expect(plsql.test_package.boolean_variable).to be_truthy
      plsql.test_package.boolean_variable = false
      expect(plsql.test_package.boolean_variable).to be_falsey
    end

  end

end
