class KBBlob
  attr_accessor :filepath, :contents

  #-----------------------------------------------------------------------
  # initialize
  #-----------------------------------------------------------------------
  def initialize(db, filepath, contents='')
    @db = db
    @filepath = filepath
    @contents = contents
  end

  #-----------------------------------------------------------------------
  # read_from_file
  #-----------------------------------------------------------------------
  def read_from_file
    @contents = @db.engine.read_blob_file(@filepath)
  end

  #-----------------------------------------------------------------------
  # write_to_file
  #-----------------------------------------------------------------------
  def write_to_file
    @db.engine.write_blob_file(@filepath, @contents)
  end
end