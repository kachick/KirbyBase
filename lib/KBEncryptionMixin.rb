# Copyright (c) 2005 NetPro Technologies, LLC
# Distributes under the same terms as Ruby License

require 'date'
require 'time'
require 'drb'
require 'fileutils'
require 'yaml'
require 'csv'

require_relative 'KBTypeConversionsMixin'


module KBEncryptionMixin
  EN_STR = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
   '0123456789.+-,$:|&;_ '
  EN_STR_LEN = EN_STR.size
  EN_KEY1 = ")2VER8GE\"87-E\n"       #*** DO NOT CHANGE ***
  EN_KEY = EN_KEY1.unpack("u")[0]
  EN_KEY_LEN = EN_KEY.size


  #-----------------------------------------------------------------------
  # encrypt_str
  #-----------------------------------------------------------------------
  #++
  # Returns an encrypted string, using the Vignere Cipher.
  #
  def encrypt_str(s)
    new_str = ''
    i_key = -1
    s.each_byte do |c|
      if i_key < EN_KEY_LEN - 1
        i_key += 1
      else
        i_key = 0
      end

      if EN_STR.index(c.chr).nil?
        new_str << c.chr
        next
      end

      i_from_str = EN_STR.index(EN_KEY[i_key]) + EN_STR.index(c.chr)
      i_from_str = i_from_str - EN_STR_LEN if i_from_str >= EN_STR_LEN
      new_str << EN_STR[i_from_str]
    end
    return new_str
  end

  #-----------------------------------------------------------------------
  # unencrypt_str
  #-----------------------------------------------------------------------
  #++
  # Returns an unencrypted string, using the Vignere Cipher.
  #
  def unencrypt_str(s)
    new_str = ''
    i_key = -1
    s.each_byte do |c|
      if i_key < EN_KEY_LEN - 1
        i_key += 1
      else
        i_key = 0
      end

      if EN_STR.index(c.chr).nil?
        new_str << c.chr
        next
      end

      i_from_str = EN_STR.index(c.chr) - EN_STR.index(EN_KEY[i_key])
      i_from_str = i_from_str + EN_STR_LEN if i_from_str < 0
      new_str << EN_STR[i_from_str]
    end
    return new_str
  end
end