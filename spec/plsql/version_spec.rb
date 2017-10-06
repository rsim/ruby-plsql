require 'spec_helper'

describe "Version" do
  it "should return ruby-plsql version" do
    expect(PLSQL::VERSION).to eq(File.read(File.dirname(__FILE__)+'/../../VERSION').chomp)
  end
  
end