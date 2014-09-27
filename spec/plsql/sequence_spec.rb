require 'spec_helper'

describe "Table" do
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
      expect(PLSQL::Sequence.find(plsql, :test_employees_seq)).not_to be_nil
    end

    it "should not find nonexisting table" do
      expect(PLSQL::Sequence.find(plsql, :qwerty123456)).to be_nil
    end

    it "should find existing sequence in schema" do
      expect(plsql.test_employees_seq).to be_a(PLSQL::Sequence)
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
      expect(PLSQL::Sequence.find(plsql, :test_employees_seq_synonym)).not_to be_nil
    end

    it "should find sequence using synonym in schema" do
      expect(plsql.test_employees_seq_synonym).to be_a(PLSQL::Sequence)
    end

  end

  describe "values" do
    it "should get next value from sequence" do
      next_value = plsql.select_one "SELECT test_employees_seq.NEXTVAL FROM dual"
      expect(plsql.test_employees_seq.nextval).to eq(next_value + 1)
    end

    it "should get current value from sequence" do
      next_value = plsql.test_employees_seq.nextval
      expect(plsql.test_employees_seq.currval).to eq(next_value)
    end
  end

end