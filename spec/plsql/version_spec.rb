require File.dirname(__FILE__) + '/../spec_helper'

describe "Version" do
  it "should return ruby-plsql version" do
    PLSQL::VERSION.should == File.read(File.dirname(__FILE__)+'/../../VERSION').chomp
  end
  
end