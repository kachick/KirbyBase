module Kernel
  def kb_nil
    KBNilClass.new
  end
end

class Object
  def full_const_get(name)
    list = name.split("::")
    obj = Object
    list.each {|x| obj = obj.const_get(x) }
    obj
  end

  def kb_nil?
    false
  end
end

class Symbol
  #-----------------------------------------------------------------------
  # -@
  #-----------------------------------------------------------------------
  #
  # This allows you to put a minus sign in front of a field name in order
  # to specify descending sort order.
  def -@
    ("-"+self.to_s).to_sym
  end

  #-----------------------------------------------------------------------
  # +@
  #-----------------------------------------------------------------------
  #
  # This allows you to put a plus sign in front of a field name in order
  # to specify ascending sort order.
  def +@
    ("+"+self.to_s).to_sym
  end
end