require 'spec_helper'

describe "Postgres Sequence" do
  
  before(:all) do
    plsql(:pg).connection = PLSQL::Connection.create(get_connection(:dialect => :postgres), :dialect => :postgres)
    #plsql(:pg).connection.autocommit = false
    plsql(:pg).execute "CREATE SEQUENCE test_employees_seq"
  end

  after(:all) do
    plsql(:pg).execute "DROP SEQUENCE test_employees_seq"
    plsql(:pg).logoff
  end

  after(:each) do
    plsql(:pg).rollback
  end

  describe "find" do

    it "should find existing sequence" do
      PLSQL::Sequence.find(plsql(:pg), :test_employees_seq).should_not be_nil
    end

    it "should not find nonexisting table" do
      PLSQL::Sequence.find(plsql(:pg), :qwerty123456).should be_nil
    end

    it "should find existing sequence in schema" do
      plsql(:pg).test_employees_seq.should be_a(PLSQL::Sequence)
    end

  end

  describe "synonym" do

    it "should find synonym to sequence" do
      pending "synonyms not supported in Postgres"
    end

    it "should find sequence using synonym in schema" do
      pending "synonyms not supported in Postgres"
    end

  end

  describe "values" do
    it "should get next value from sequence" do
      next_value = plsql(:pg).select_one "SELECT nextval('test_employees_seq')"
      plsql(:pg).test_employees_seq.nextval.should == next_value + 1
    end

    it "should get current value from sequence" do
      next_value = plsql(:pg).test_employees_seq.nextval
      plsql(:pg).test_employees_seq.currval.should == next_value
    end
  end

end