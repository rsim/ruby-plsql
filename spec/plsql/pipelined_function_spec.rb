require 'spec_helper'

describe "PipelinedFunction" do
  before(:all) do
    plsql.connection = get_connection
    # Not use ROWTYPE definition due to Oracle bug (see http://arjudba.blogspot.com/2011/12/ora-00600-internal-error-code-arguments.html)
    plsql.execute(<<-SQL)
      CREATE OR REPLACE PACKAGE test_package IS
        TYPE object_record IS RECORD(
          owner        ALL_OBJECTS.OWNER%TYPE,
          object_name  ALL_OBJECTS.OBJECT_NAME%TYPE,
          object_id    ALL_OBJECTS.OBJECT_ID%TYPE,
          object_type  ALL_OBJECTS.OBJECT_TYPE%TYPE);
        TYPE objects_list IS TABLE OF object_record;

        FUNCTION find_objects_by_name ( p_name ALL_OBJECTS.OBJECT_NAME%TYPE )
          RETURN objects_list PIPELINED;
      END test_package;
    SQL

    plsql.execute(<<-SQL)
      CREATE OR REPLACE PACKAGE BODY test_package IS
        FUNCTION find_objects_by_name ( p_name ALL_OBJECTS.OBJECT_NAME%TYPE )
          RETURN objects_list PIPELINED
        IS
        BEGIN
          FOR l_object IN (
            SELECT OWNER, OBJECT_NAME, OBJECT_ID, OBJECT_TYPE
            FROM   ALL_OBJECTS
            WHERE  OBJECT_NAME LIKE UPPER(p_name)
            AND    ROWNUM < 11)
          LOOP
            PIPE ROW(l_object);
          END LOOP;
        END find_objects_by_name;
      END;
    SQL

    plsql.execute(<<-SQL)
      CREATE OR REPLACE TYPE test_numbers AS TABLE OF NUMBER;
    SQL

    plsql.execute(<<-SQL)
      CREATE OR REPLACE
      FUNCTION test_pipelined_func (p_high NUMBER)
        RETURN test_numbers PIPELINED
      IS
        l_cnt NUMBER := p_high;
      BEGIN
        LOOP
          PIPE ROW(l_cnt);
          l_cnt := l_cnt - 1;
          EXIT WHEN l_cnt < 0;
        END LOOP;
      END test_pipelined_func;
    SQL
  end

  after(:all) do
    plsql.execute "DROP PACKAGE test_package"
    plsql.execute "DROP FUNCTION test_pipelined_func"
    plsql.execute "DROP TYPE test_numbers"
    plsql.logoff
  end

  it "should identify arguments" do
    arguments = plsql.test_package['find_objects_by_name'].arguments
    arguments.should be_an Enumerable
    arguments.size.should == 1
  end

  it "should identify returning table type" do
    return_type = plsql.test_package['find_objects_by_name'].return
    return_type[:data_type].should == 'TABLE'
  end

  it "should returns an array of hashes on exec" do
    duals = plsql.test_package.find_objects_by_name(:p_name => 'DUAL')
    duals.size.should == 2
    duals[0].should be_a Hash
    duals[0]['object_name'].should == 'DUAL'

    numbers = PLSQL::PipelinedFunction.find(plsql, :test_pipelined_func).exec(10)
    numbers.size.should == 11
    numbers.last['column_value'].should == 0
  end

  it "should iterate over result set if block given" do
    sys_objects = []
    plsql.test_package.find_objects_by_name(:p_name => '%#') {|row| sys_objects << row}
    sys_objects.size.should == 10
    sys_objects.map{|object| object['object_name'][-1..-1]}.uniq.should == %w(#)
  end
end