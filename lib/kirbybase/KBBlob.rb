class KBBlob

  attr_accessor :filepath, :contents

  def initialize(db, filepath, contents='')
    @db = db
    @filepath = filepath
    @contents = contents
  end

  def read_from_file
    @contents = @db.engine.read_blob_file(@filepath)
  end

  def write_to_file
    @db.engine.write_blob_file(@filepath, @contents)
  end

end