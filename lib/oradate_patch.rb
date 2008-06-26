class OraDate
  if defined? DateTime # ruby 1.8.0 or upper
    # RSI: create DateTime in local timezone
    def to_datetime
      DateTime.parse(Time.local(year, month, day, hour, minute, second).iso8601)
    rescue
      DateTime.new(year, month, day, hour, minute, second)
    end
  end
end
