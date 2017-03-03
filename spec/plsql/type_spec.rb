require 'spec_helper'

describe "Type" do
  before(:all) do
    plsql.connection = get_connection
    plsql.execute "DROP TYPE t_employee" rescue nil
    plsql.execute "DROP TYPE t_phones" rescue nil
    plsql.execute "DROP TYPE t_phone" rescue nil
    plsql.execute <<-SQL
      CREATE OR REPLACE TYPE t_address AS OBJECT (
        street    VARCHAR2(50),
        city      VARCHAR2(50),
        country   VARCHAR2(50),
        CONSTRUCTOR FUNCTION t_address(p_full_address VARCHAR2)
          RETURN SELF AS RESULT,
        MEMBER FUNCTION display_address(p_separator VARCHAR2 DEFAULT ',') RETURN VARCHAR2,
        MEMBER FUNCTION display_address(p_uppercase BOOLEAN, p_separator VARCHAR2 DEFAULT ',') RETURN VARCHAR2,
        MEMBER PROCEDURE set_country(p_country VARCHAR2),
        MEMBER PROCEDURE set_country2(p_country VARCHAR2, x_display_address OUT VARCHAR2),
        STATIC FUNCTION create_address(p_full_address VARCHAR2) RETURN t_address
      );
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE TYPE BODY t_address AS
        CONSTRUCTOR FUNCTION t_address(p_full_address VARCHAR2)
          RETURN SELF AS RESULT
        AS
          l_comma1_pos INTEGER;
          l_comma2_pos INTEGER;
        BEGIN
          l_comma1_pos := INSTR(p_full_address, ',', 1, 1);
          l_comma2_pos := INSTR(p_full_address, ',', 1, 2);
          SELF.street := TRIM(SUBSTR(p_full_address, 1, l_comma1_pos - 1));
          SELF.city := TRIM(SUBSTR(p_full_address, l_comma1_pos+1, l_comma2_pos - l_comma1_pos - 1));
          SELF.country := TRIM(SUBSTR(p_full_address, l_comma2_pos+1));
          RETURN;
        END;
        MEMBER FUNCTION display_address(p_separator VARCHAR2) RETURN VARCHAR2 IS
          l_separator VARCHAR2(10) := p_separator;
        BEGIN
          IF SUBSTR(l_separator,-1) != ' ' THEN
            l_separator := l_separator || ' ';
          END IF;
          RETURN SELF.street || l_separator || SELF.city || l_separator || SELF.country;
        END;
        MEMBER FUNCTION display_address(p_uppercase BOOLEAN, p_separator VARCHAR2) RETURN VARCHAR2 IS
          l_separator VARCHAR2(10) := p_separator;
        BEGIN
          IF p_uppercase THEN
            RETURN UPPER(SELF.display_address(p_separator));
          ELSE
            RETURN SELF.display_address(p_separator);
          END IF;
        END;
        MEMBER PROCEDURE set_country(p_country VARCHAR2) IS
        BEGIN
          SELF.country := p_country;
        END;
        MEMBER PROCEDURE set_country2(p_country VARCHAR2, x_display_address OUT VARCHAR2) IS
        BEGIN
          SELF.country := p_country;
          x_display_address := SELF.display_address();
        END;
        STATIC FUNCTION create_address(p_full_address VARCHAR2) RETURN t_address IS
        BEGIN
          RETURN t_address(p_full_address);
        END;
      END;
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE TYPE t_phone AS OBJECT (
        type            VARCHAR2(10),
        phone_number    VARCHAR2(50)
      )
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE TYPE t_phones AS VARRAY(10) OF T_PHONE
    SQL
    plsql.execute <<-SQL
      CREATE OR REPLACE TYPE t_employee AS OBJECT (
        employee_id   NUMBER(15),
        first_name    VARCHAR2(50),
        last_name     VARCHAR(50),
        hire_date     DATE,
        address       t_address,
        phones        t_phones
      )
    SQL
  end

  after(:all) do
    plsql.execute "DROP TYPE t_employee"
    plsql.execute "DROP TYPE t_address"
    plsql.execute "DROP TYPE t_phones"
    plsql.execute "DROP TYPE t_phone"
    plsql.logoff
  end

  after(:each) do
    plsql.rollback
  end

  describe "find" do

    it "should find existing type" do
      expect(PLSQL::Type.find(plsql, :t_employee)).not_to be_nil
    end

    it "should not find nonexisting type" do
      expect(PLSQL::Type.find(plsql, :qwerty123456)).to be_nil
    end

    it "should find existing type in schema" do
      expect(plsql.t_employee).to be_a(PLSQL::Type)
    end

  end

  describe "synonym" do

    before(:all) do
      plsql.execute "CREATE SYNONYM t_employee_synonym FOR hr.t_employee"
    end

    after(:all) do
      plsql.execute "DROP SYNONYM t_employee_synonym" rescue nil
    end

    it "should find synonym to type" do
      expect(PLSQL::Type.find(plsql, :t_employee_synonym)).not_to be_nil
    end

    it "should find type using synonym in schema" do
      expect(plsql.t_employee_synonym).to be_a(PLSQL::Type)
    end

  end

  describe "public synonym" do

    it "should find public synonym to type" do
      expect(PLSQL::Type.find(plsql, :xmltype)).not_to be_nil
    end

    it "should find type using public synonym in schema" do
      expect(plsql.xmltype).to be_a(PLSQL::Type)
    end

  end

  describe "typecode" do

    it "should get typecode of object type" do
      expect(plsql.t_employee.typecode).to eq("OBJECT")
    end

    it "should get typecode of collection type" do
      expect(plsql.t_phones.typecode).to eq("COLLECTION")
    end

  end

  describe "attributes" do

    it "should get attribute names" do
      expect(plsql.t_employee.attribute_names).to eq([:employee_id, :first_name, :last_name, :hire_date, :address, :phones])
    end

    it "should get attributes metadata" do
      expect(plsql.t_employee.attributes).to eq({
        :employee_id =>
          {:position=>1, :data_type=>"NUMBER", :data_length=>nil, :data_precision=>15, :data_scale=>0, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :first_name =>
          {:position=>2, :data_type=>"VARCHAR2", :data_length=>50, :data_precision=>nil, :data_scale=>nil, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :last_name =>
          {:position=>3, :data_type=>"VARCHAR2", :data_length=>50, :data_precision=>nil, :data_scale=>nil, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :hire_date => 
          {:position=>4, :data_type=>"DATE", :data_length=>nil, :data_precision=>nil, :data_scale=>nil, :type_owner=>nil, :type_name=>nil, :sql_type_name=>nil},
        :address => 
          {:position=>5, :data_type=>"OBJECT", :data_length=>nil, :data_precision=>nil, :data_scale=>nil, :type_owner=>"HR", :type_name=>"T_ADDRESS", :sql_type_name=>"HR.T_ADDRESS"},
        :phones => 
          {:position=>6, :data_type=>"TABLE", :data_length=>nil, :data_precision=>nil, :data_scale=>nil, :type_owner=>"HR", :type_name=>"T_PHONES", :sql_type_name=>"HR.T_PHONES"}
      })
    end

  end

  describe "object instance" do
    before(:all) do
      @phone_attributes = {:type => 'mobile', :phone_number => '123456'}
      @address_attributes = {:street => 'Street', :city => 'City', :country => 'Country'}
      @full_address = "#{@address_attributes[:street]}, #{@address_attributes[:city]}, #{@address_attributes[:country]}"
    end

    it "should get new object instance using named parameters" do
      expect(plsql.t_phone(@phone_attributes)).to eq(@phone_attributes)
    end

    it "should be an ObjectInstance" do
      expect(plsql.t_phone(@phone_attributes)).to be_a(PLSQL::ObjectInstance)
    end

    it "should get new object instance using sequential parameters" do
      expect(plsql.t_phone(@phone_attributes[:type], @phone_attributes[:phone_number])).to eq(@phone_attributes)
    end

    it "should get new object instance using custom constructor" do
      expect(plsql.t_address(@full_address)).to eq(@address_attributes)
      expect(plsql.t_address(:p_full_address => @full_address)).to eq(@address_attributes)
    end

    it "should get new object instance using default constructor when custom constructor exists" do
      expect(plsql.t_address(@address_attributes)).to eq(@address_attributes)
      expect(plsql.t_address(@address_attributes[:street], @address_attributes[:city], @address_attributes[:country])).to eq(@address_attributes)
    end

    it "should get new empty collection of objects instance" do
      expect(plsql.t_phones.new).to eq([])
      expect(plsql.t_phones([])).to eq([])
    end

    it "should get new collection of objects instances" do
      phone = plsql.t_phone(@phone_attributes)
      expect(plsql.t_phones([phone, phone])).to eq([phone, phone])
      expect(plsql.t_phones(phone, phone)).to eq([phone, phone])
      expect(plsql.t_phones(@phone_attributes, @phone_attributes)).to eq([phone, phone])
    end

  end

  describe "member procedures" do
    before(:all) do
      @address_attributes = {:street => 'Street', :city => 'City', :country => 'Country'}
      @full_address = "#{@address_attributes[:street]}, #{@address_attributes[:city]}, #{@address_attributes[:country]}"
    end

    it "should call object instance member function without parameters" do
      expect(plsql.t_address(@address_attributes).display_address).to eq(@full_address)
    end

    it "should call object instance member function with parameters" do
      expect(plsql.t_address(@address_attributes).display_address(',')).to eq(@full_address)
    end

    it "should call object instance member function with named parameters" do
      expect(plsql.t_address(@address_attributes).display_address(:p_separator => ',')).to eq(@full_address)
    end

    it "should call object overloaded instance member function" do
      expect(plsql.t_address(@address_attributes).display_address(true)).to eq(@full_address.upcase)
      expect(plsql.t_address(@address_attributes).display_address(true, ',')).to eq(@full_address.upcase)
    end

    it "should call object instance member function with explicit first SELF parameter" do
      expect(plsql.t_address.display_address(@address_attributes, ',')).to eq(@full_address)
    end

    it "should call object instance member function with explicit named SELF parameter" do
      expect(plsql.t_address.display_address(:self => @address_attributes, :p_separator => ',')).to eq(@full_address)
    end

    it "should call object instance member procedure" do
      other_country = "Other"
      expect(plsql.t_address(@address_attributes).set_country(other_country)).to eq(@address_attributes.merge(:country => other_country))
    end

    it "should call object instance member procedure with output parameters" do
      other_country = "Other"
      expect(plsql.t_address(@address_attributes).set_country2(other_country)).to eq(
        [@address_attributes.merge(:country => other_country),
        {:x_display_address => "#{@address_attributes[:street]}, #{@address_attributes[:city]}, #{other_country}"}]
      )
    end

    it "should raise error if invalid member procedure is called" do
      expect do
        plsql.t_address(@address_attributes).invalid_procedure
      end.to raise_error(ArgumentError)
    end

  end

  describe "static procedures" do
    before(:all) do
      @address_attributes = {:street => 'Street', :city => 'City', :country => 'Country'}
      @full_address = "#{@address_attributes[:street]}, #{@address_attributes[:city]}, #{@address_attributes[:country]}"
    end

    it "should call object type static function" do
      expect(plsql.t_address.create_address(@full_address)).to eq(@address_attributes)
    end

    it "should call object type static function with named parameters" do
      expect(plsql.t_address.create_address(:p_full_address => @full_address)).to eq(@address_attributes)
    end

    it "should raise error if invalid static procedure is called" do
      expect do
        plsql.t_address.invalid_procedure
      end.to raise_error(ArgumentError)
    end

  end

end
