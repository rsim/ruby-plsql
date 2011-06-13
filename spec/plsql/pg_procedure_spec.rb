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
        CREATE OR REPLACE FUNCTION test_uppercase(p_string VARCHAR)
          RETURNS VARCHAR
        AS $$
        BEGIN
          RETURN UPPER(p_string);
        END;
        $$ LANGUAGE 'plpgsql';
      SQL
    end
  
    after(:all) do
      plsql(:pg).execute "DROP FUNCTION test_uppercase(varchar)"
    end
  
    it "should find existing procedure" do
      PLSQL::Procedure.find(plsql(:pg), :test_uppercase).should_not be_nil
    end

    it "should not find nonexisting procedure" do
      PLSQL::Procedure.find(plsql(:pg), :qwerty123456).should be_nil
    end

  end
end