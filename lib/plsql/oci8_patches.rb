# apply TIMESTAMP fractional seconds patch to ruby-oci8 2.0.3
# see http://rubyforge.org/forum/forum.php?thread_id=46576&forum_id=1078
if OCI8::VERSION == "2.0.3" &&
  !OCI8::BindType::Util.method_defined?(:datetime_to_array_without_timestamp_patch)

  OCI8::BindType::Util.module_eval do
    alias :datetime_to_array_without_timestamp_patch :datetime_to_array
    def datetime_to_array(val, full)
      result = datetime_to_array_without_timestamp_patch(val, full)
      if result && result[6] == 0
        if val.respond_to? :nsec
          fsec = val.nsec
        elsif val.respond_to? :usec
          fsec = val.usec * 1000
        else
          fsec = 0
        end
        result[6] = fsec
      end
      result
    end
    private :datetime_to_array_without_timestamp_patch, :datetime_to_array
  end

end
