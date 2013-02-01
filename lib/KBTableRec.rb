class KBTableRec
  include KBTypeConversionsMixin

  def initialize(tbl)
    @tbl = tbl
  end

  def populate(rec)
    @tbl.field_names.zip(rec).each do |fn, val|
      send("#{fn}=", val)
    end
  end

  def clear
    @tbl.field_names.each do |fn|
      send("#{fn}=", kb_nil)
    end
  end
end