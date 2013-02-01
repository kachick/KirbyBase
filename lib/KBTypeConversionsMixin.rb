# Copyright (c) 2005 NetPro Technologies, LLC
# Distributes under the same terms as Ruby License

require 'date'
require 'time'
require 'drb'
require 'fileutils'
require 'yaml'
require 'csv'

module KBTypeConversionsMixin
  # Constant that will represent a kb_nil in the physical table file.
  # If you think you might need to write the value 'kb_nil' to a field
  # yourself, comment out the following line and un-comment the line
  # below that to use an alternative representation for kb_nil.
  KB_NIL = 'kb_nil'
  #KB_NIL = '&kb_nil;'

  # Regular expression used to determine if field needs to be un-encoded.
  UNENCODE_RE = /&(?:amp|linefeed|carriage_return|substitute|pipe);/

  # Regular expression used to determine if field needs to be encoded.
  ENCODE_RE = /&|\n|\r|\032|\|/

  #-----------------------------------------------------------------------
  # convert_to_native_type
  #-----------------------------------------------------------------------
  #++
  # Return value converted from storage string to native field type.
  #
  def convert_to_native_type(data_type, s)
    return kb_nil if s == KB_NIL

    # I added this line to keep KBTable#import_csv working after I made
    # the kb_nil changes.
    return nil if s.nil?

    case data_type
    when :String
      if s =~ UNENCODE_RE
        return s.gsub('&linefeed;', "\n").gsub('&carriage_return;',
         "\r").gsub('&substitute;', "\032").gsub('&pipe;', "|"
         ).gsub('&amp;', "&")
      else
        return s
      end
    when :Integer
      return s.to_i
    when :Float
      return s.to_f
    when :Boolean
      if ['false', 'False', nil, false].include?(s)
        return false
      else
        return true
      end
    when :Time
      return Time.parse(s)
    when :Date
      return Date.parse(s)
    when :DateTime
      return DateTime.parse(s)
    when :YAML
      # This code is here in case the YAML field is the last
      # field in the record.  Because YAML normally defines a
      # nil value as "--- ", but KirbyBase strips trailing
      # spaces off the end of the record, so if this is the
      # last field in the record, KirbyBase will strip the
      # trailing space off and make it "---".  When KirbyBase
      # attempts to convert this value back using to_yaml,
      # you get an exception.
      if s == "---"
        return nil
      elsif s =~ UNENCODE_RE
        y = s.gsub('&linefeed;', "\n").gsub('&carriage_return;',
         "\r").gsub('&substitute;', "\032").gsub('&pipe;', "|"
         ).gsub('&amp;', "&")
        return YAML.load(y)
      else
        return YAML.load(s)
      end
    when :Memo
      memo = KBMemo.new(@tbl.db, s)
      memo.read_from_file
      return memo
    when :Blob
      blob = KBBlob.new(@tbl.db, s)
      blob.read_from_file
      return blob
    else
      raise "Invalid field type: %s" % data_type
    end
  end

  #-----------------------------------------------------------------------
  # convert_to_encoded_string
  #-----------------------------------------------------------------------
  #++
  # Return value converted to encoded String object suitable for storage.
  #
  def convert_to_encoded_string(data_type, value)
    return KB_NIL if value.nil?

    case data_type
    when :YAML
      y = value.to_yaml
      if y =~ ENCODE_RE
        return y.gsub("&", '&amp;').gsub("\n", '&linefeed;').gsub(
         "\r", '&carriage_return;').gsub("\032", '&substitute;'
         ).gsub("|", '&pipe;')
      else
        return y
      end
    when :String
      if value =~ ENCODE_RE
        return value.gsub("&", '&amp;').gsub("\n", '&linefeed;'
         ).gsub("\r", '&carriage_return;').gsub("\032",
         '&substitute;').gsub("|", '&pipe;')
      else
        return value
      end
    when :Memo
      return value.filepath
    when :Blob
      return value.filepath
    else
      return value.to_s
    end
  end
end