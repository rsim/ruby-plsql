require 'spec_helper'

describe "Spatial" do
  before(:all) do
    # plsql.connect! CONNECTION_PARAMS
    @connection = OCI8.new('hr','hr','//localhost:1521/XE') # TODO use CONNECTION_PARAMS
    plsql.connection = @connection
    plsql.connection.autocommit = false
    plsql.execute <<-SQL
      CREATE TABLE test_spatial (
        geom   MDSYS.SDO_GEOMETRY
      )
    SQL

    @insert_row_2d_polygon_sql = <<-SQL
      insert into test_spatial(geom) values (
        SDO_GEOMETRY(
          2003,
          27700,
          NULL,
          SDO_ELEM_INFO_ARRAY(1, 1003, 1),
          SDO_ORDINATE_ARRAY(2, 1, 4, 1, 4, 6, 2, 6, 2, 1)
        )
      )
  SQL
    @insert_row_2d_point_sql = <<-SQL
      insert into test_spatial(geom) values (
        SDO_GEOMETRY(
          2001,
          NULL,
          SDO_POINT_TYPE(12, 14, NULL),
          NULL,
          NULL
        )
      )
  SQL
  end

  after(:all) do
    plsql.execute "DROP TABLE test_spatial"
    plsql.logoff
  end

  after(:each) do
    plsql.rollback
  end

  describe "table" do
    it "should be able to use the cursor's geometry" do
      puts "number of rows before: #{plsql.test_spatial.count}"
      geom = make_geometry(2003, 27700, nil, [1, 1003, 1], [2, 1, 4, 1, 4, 6, 2, 6, 2, 1])
      plsql.test_spatial.insert({:geom=>geom})
      expect(plsql.test_spatial.first).to eq({:geom=>{:sdo_gtype=>2003, :sdo_srid=>27700, :sdo_point=>nil, :sdo_elem_info=>[1, 1003, 1], :sdo_ordinates=>[2, 1, 4, 1, 4, 6, 2, 6, 2, 1]}})
    end
    # xit "should be able to create a geometry that can be used in an insert statement" do
    #   geom = plsql.sdo_geometry({:sdo_gtype=>2001, :sdo_srid=>nil, :sdo_point=>{:x=>12, :y=>14, :z=>nil}, :sdo_elem_info=>nil, :sdo_ordinates=>nil})
    #   plsql.test_spatial.insert({:geom=>geom})
    # end
    # xit "should select a polygon from a table" do
    #   plsql.execute @insert_row_2d_polygon_sql
    #   expect(plsql.test_spatial.first).to eq({:geom=>{:sdo_gtype=>2003, :sdo_srid=>27700, :sdo_point=>nil, :sdo_elem_info=>[1, 1003, 1], :sdo_ordinates=>[2, 1, 4, 1, 4, 6, 2, 6, 2, 1]}})
    # end
    # xit "should select a point from a table" do
    #   plsql.execute @insert_row_2d_point_sql
    #   expect(plsql.test_spatial.first).to eq({:geom=>{:sdo_gtype=>2001, :sdo_srid=>nil, :sdo_point=>{:x=>12, :y=>14, :z=>nil}, :sdo_elem_info=>nil, :sdo_ordinates=>nil}})
    # end
    # xit "should insert a row" do
    #   plsql.test_spatial.insert({:geom=>{:sdo_gtype=>2001, :sdo_srid=>nil, :sdo_point=>{:x=>12, :y=>14, :z=>nil}, :sdo_elem_info=>nil, :sdo_ordinates=>nil}})
    # end
  end
end

def make_geometry(gtype, srid, sdo_point, sdo_elem_info_array, sdo_ordinate_array)
  # based on some of the logic from http://stackoverflow.com/a/11323760
  local_cursor = @connection.parse <<-SQL
    begin
      :geom := SDO_GEOMETRY(:sdo_gtype, :sdo_srid, :sdo_point, :sdo_elem_info_array, :sdo_ordinate_array);
    end;
  SQL
  local_cursor.bind_param(:sdo_gtype, gtype, OraNumber)
  local_cursor.bind_param(:sdo_srid, srid, OraNumber)
  local_cursor.bind_param(:sdo_point, sdo_point, OCI8::Object::Mdsys::SdoPointType)
  local_cursor.bind_param(:sdo_elem_info_array, sdo_elem_info_array, OCI8::Object::Mdsys::SdoElemInfoArray)
  local_cursor.bind_param(:sdo_ordinate_array, sdo_ordinate_array, OCI8::Object::Mdsys::SdoOrdinateArray)
  local_cursor.bind_param(:geom, OCI8::Object::Mdsys::SdoGeometry)
  local_cursor.exec
  local_cursor[:geom]
end

# these need to be defined before newing up will work:
# based on information from https://github.com/kubo/ruby-oci8/issues/37#issuecomment-19814547
module OCI8::Object::Mdsys
  class SdoPointType < OCI8::Object::Base
  end
  class SdoElemInfoArray < OCI8::Object::Base
  end
  class SdoOrdinateArray < OCI8::Object::Base
  end
  class SdoGeometry < OCI8::Object::Base
    set_typename('MDSYS.SDO_GEOMETRY')
  end
end
