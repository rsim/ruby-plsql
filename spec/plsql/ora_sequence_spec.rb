require 'spec_helper'

describe "Oracle Sequence" do
  
  before(:all) do
    plsql.connection = get_connection
    plsql.connection.autocommit = false
    plsql.execute "CREATE SEQUENCE test_employees_seq"
  end

  after(:all) do
    plsql.execute "DROP SEQUENCE test_employees_seq"
    plsql.logoff
  end

  after(:each) do
    plsql.rollback
  end

  describe "find" do

    it "should find existing sequence" do
      PLSQL::Sequence.find(plsql, :test_employees_seq).should_not be_nil
    end

    it "should not find nonexisting table" do
      PLSQL::Sequence.find(plsql, :qwerty123456).should be_nil
    end

    it "should find existing sequence in schema" do
      plsql.test_employees_seq.should be_a(PLSQL::Sequence)
    end

  end

  describe "synonym" do

    before(:all) do
      plsql.connection.exec "CREATE SYNONYM test_employees_seq_synonym FOR hr.test_employees_seq"
    end

    after(:all) do
      plsql.connection.exec "DROP SYNONYM test_employees_seq_synonym" rescue nil
    end

    it "should find synonym to sequence" do
      PLSQL::Sequence.find(plsql, :test_employees_seq_synonym).should_not be_nil
    end

    it "should find sequence using synonym in schema" do
      plsql.test_employees_seq_synonym.should be_a(PLSQL::Sequence)
    end

  end

  describe "values" do
    it "should get next value from sequence" do
      next_value = plsql.select_one "SELECT test_employees_seq.NEXTVAL FROM dual"
      plsql.test_employees_seq.nextval.should == next_value + 1
    end

    it "should get current value from sequence" do
      next_value = plsql.test_employees_seq.nextval
      plsql.test_employees_seq.currval.should == next_value
    end
  end

end