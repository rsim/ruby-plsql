# encoding: utf-8

require File.dirname(__FILE__) + '/../spec_helper'

describe "Package variables /" do

  before(:all) do
    plsql.connection = get_connection
  end

  after(:all) do
    plsql.logoff
  end

  describe "String" do
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          varchar2_variable VARCHAR2(50);
          varchar2_default varchar2(50) := 'default' ;
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
    end

    it "should set and get VARCHAR2 variable" do
      plsql.test_package.varchar2_variable = 'abc'
      plsql.test_package.varchar2_variable.should == 'abc'
    end

    it "should get VARCHAR2 variable default value" do
      plsql.test_package.varchar2_default.should == 'default'
    end

    it "should set and get CHAR variable" do
      plsql.test_package.char_variable = 'abc'
      plsql.test_package.char_variable.should == 'abc' + ' '*7
    end

    it "should set and get NVARCHAR2 variable" do
      plsql.test_package.nvarchar2_variable = 'abc'
      plsql.test_package.nvarchar2_variable.should == 'abc'
    end

    it "should set and get NCHAR variable" do
      plsql.test_package.nchar_variable = 'abc'
      plsql.test_package.nchar_variable.should == 'abc' + ' '*7
    end

  end

  describe "Numeric" do
    before(:all) do
      plsql.execute <<-SQL
        CREATE OR REPLACE PACKAGE test_package IS
          integer_variable NUMBER(10);
          integer_default NUMBER(10) := 1;
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
    end

    it "should set and get integer variable" do
      plsql.test_package.integer_variable = 1
      plsql.test_package.integer_variable.should be_a Fixnum
      plsql.test_package.integer_variable.should == 1
    end

    it "should get integer variable default value" do
      plsql.test_package.integer_default.should == 1
    end

    it "should set and get PLS_INTEGER variable" do
      plsql.test_package.pls_int_variable = 1
      plsql.test_package.pls_int_variable.should be_a Fixnum
      plsql.test_package.pls_int_variable.should == 1
    end

    it "should set and get BINARY_INTEGER variable" do
      plsql.test_package.bin_int_variable = 1
      plsql.test_package.bin_int_variable.should be_a Fixnum
      plsql.test_package.bin_int_variable.should == 1
    end

    it "should set and get NUMBER variable" do
      plsql.test_package.number_variable = 123.456
      plsql.test_package.number_variable.should be_a BigDecimal
      plsql.test_package.number_variable.should == 123.456
    end

    it "should set and get NUMBER variable with scale" do
      plsql.test_package.number_with_scale = 123.456
      plsql.test_package.number_with_scale.should == 123.46 # rounding to two decimal digits
    end

  end

  describe "Date and Time" do
    before(:all) do
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
    end

    it "should set and get DATE variable" do
      plsql.test_package.date_variable = @date
      plsql.test_package.date_variable.should be_a Time
      plsql.test_package.date_variable.should == @date
    end

    it "should get DATE variable default value" do
      plsql.test_package.date_default.should == @date
    end

    it "should set and get TIMESTAMP variable" do
      plsql.test_package.timestamp_variable = @timestamp
      plsql.test_package.timestamp_variable.should be_a Time
      plsql.test_package.timestamp_variable.should == @timestamp
    end

    it "should set and get TIMESTAMP WITH TIME ZONE variable" do
      plsql.test_package.timestamptz_variable = @timestamp
      plsql.test_package.timestamptz_variable.should be_a Time
      plsql.test_package.timestamptz_variable.should == @timestamp
    end

    it "should set and get TIMESTAMP WITH LOCAL TIME ZONE variable" do
      plsql.test_package.timestampltz_variable = @timestamp
      plsql.test_package.timestampltz_variable.should be_a Time
      plsql.test_package.timestampltz_variable.should == @timestamp
    end

  end

  describe "LOB" do
    before(:all) do
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
    end

    it "should set and get CLOB variable" do
      plsql.test_package.clob_variable = 'abc'
      plsql.test_package.clob_variable.should == 'abc'
    end

    it "should get CLOB variable default value" do
      plsql.test_package.clob_default.should == 'default'
    end

    it "should set and get NCLOB variable" do
      plsql.test_package.nclob_variable = 'abc'
      plsql.test_package.nclob_variable.should == 'abc'
    end

    it "should set and get BLOB variable" do
      plsql.test_package.blob_variable = "\000\001\003"
      plsql.test_package.blob_variable.should == "\000\001\003"
    end

  end

end