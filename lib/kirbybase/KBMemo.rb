class KBMemo

  attr_accessor :filepath, :contents

  def initialize(db, filepath, contents='')
    @db = db
    @filepath = filepath
    @contents = contents
  end

  def read_from_file
    @contents = @db.engine.read_memo_file(@filepath)
  end

  def write_to_file
    @db.engine.write_memo_file(@filepath, @contents)
  end

end