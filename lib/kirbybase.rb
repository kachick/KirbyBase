# Copyright (c) 2005 NetPro Technologies, LLC
# Distributes under the same terms as Ruby License

require 'date'
require 'time'
require 'drb'
require 'fileutils'
require 'yaml'
require 'csv'

require 'optionalargument'
require 'striuct'

module KirbyBase

  VERSION = "3.0.0.dev".freeze

  def self.new(*args, &block)
    self::Base.__send__(__callee__, *args, &block)
  end

end

require_relative 'kirbybase/KBTypeConversionsMixin'
require_relative 'kirbybase/KBEncryptionMixin'
require_relative 'kirbybase/base'
require_relative 'kirbybase/KBEngine'
require_relative 'kirbybase/KBTable'
require_relative 'kirbybase/KBMemo'
require_relative 'kirbybase/KBBlob'
require_relative 'kirbybase/KBIndex'
require_relative 'kirbybase/KBRecnoIndex'
require_relative 'kirbybase/KBTableRec'
require_relative 'kirbybase/KBResultSet'
require_relative 'kirbybase/KBNilClass'
require_relative 'kirbybase/core_ext'