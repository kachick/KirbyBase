class KBNilClass
  include Comparable

  class << self
    def new
      @kb_nil ||= KBNilClass.allocate
    end
  end

  def inspect
    'kb_nil'
  end

  def kb_nil?
    true
  end

  def to_s
    ""
  end

  def to_i
    0
  end

  def to_f
    0.0
  end

  def to_a
    []
  end

  def <=>(other)
    return 0 if other.kb_nil?
    return -1
  end

  def coerce(other)
    return [other, to_i] if other.kind_of? Fixnum
    return [other, to_f] if other.kind_of? Float

    raise "Didn't know how to coerce kb_nil to a #{other.class}"
  end

  def method_missing(sym, *args)
    case sym
    when :to_str, :to_ary
      super
    else
      self
    end
  end
end