class KBEngine

  include DRb::DRbUndumped
  include KBTypeConversionsMixin
  include KBEncryptionMixin

  # Make constructor private.
  private_class_method :new

  def KBEngine.create_called_from_database_instance(db)
    new(db)
  end

  def initialize(db)
    @db = db
    @recno_indexes = {}
    @indexes = {}

    # This hash will hold the table locks if in client/server mode.
    @mutex_hash = {} if @db.server?
  end

  def init_recno_index(table)
    return if recno_index_exists?(table)

    with_write_locked_table(table) do |fptr|
      @recno_indexes[table.name] = KBRecnoIndex.new(table)
      @recno_indexes[table.name].rebuild(fptr)
    end
  end

  def rebuild_recno_index(table)
    with_write_locked_table(table) do |fptr|
      @recno_indexes[table.name].rebuild(fptr)
    end
  end

  def remove_recno_index(tablename)
    @recno_indexes.delete(tablename)
  end

  def update_recno_index(table, recno, fpos)
    @recno_indexes[table.name].update_index_rec(recno, fpos)
  end

  def recno_index_exists?(table)
    @recno_indexes.include?(table.name)
  end

  def get_recno_index(table)
    return @recno_indexes[table.name].get_idx
  end

  def init_index(table, index_fields)
    return if index_exists?(table, index_fields)

    with_write_locked_table(table) do |fptr|
      @indexes["#{table.name}_#{index_fields.join('_')}".to_sym] = \
       KBIndex.new(table, index_fields)
      @indexes["#{table.name}_#{index_fields.join('_')}".to_sym
       ].rebuild(fptr)
    end
  end

  def rebuild_index(table, index_fields)
    with_write_locked_table(table) do |fptr|
      @indexes["#{table.name}_#{index_fields.join('_')}".to_sym
       ].rebuild(fptr)
    end
  end

  def remove_indexes(tablename)
    re_table_name = Regexp.new(tablename.to_s)
    @indexes.delete_if { |k,v| k.to_s =~ re_table_name }
  end

  def add_to_indexes(table, rec, fpos)
    @recno_indexes[table.name].add_index_rec(rec.first, fpos)

    re_table_name = Regexp.new(table.name.to_s)
    @indexes.each_pair do |key, index|
      index.add_index_rec(rec) if key.to_s =~ re_table_name
    end
  end

  def delete_from_indexes(table, rec, fpos)
    @recno_indexes[table.name].delete_index_rec(rec.recno)

    re_table_name = Regexp.new(table.name.to_s)
    @indexes.each_pair do |key, index|
      index.delete_index_rec(rec.recno) if key.to_s =~ re_table_name
    end
  end

  def update_to_indexes(table, rec)
    re_table_name = Regexp.new(table.name.to_s)
    @indexes.each_pair do |key, index|
      index.update_index_rec(rec) if key.to_s =~ re_table_name
    end
  end

  def index_exists?(table, index_fields)
    @indexes.include?("#{table.name}_#{index_fields.join('_')}".to_sym)
  end

  def get_index(table, index_name)
    @indexes["#{table.name}_#{index_name}".to_sym].get_idx
  end

  def get_index_timestamp(table, index_name)
    @indexes["#{table.name}_#{index_name}".to_sym].get_timestamp
  end

  def table_exists?(tablename)
    File.exists?(File.join(@db.path, tablename.to_s + @db.ext))
  end

  def tables
    list = []
    Dir.foreach(@db.path) { |filename|
      list << File.basename(filename, '.*').to_sym if \
       File.extname(filename) == @db.ext
    }
    return list
  end

  # Create physical file holding table. This table should not be directly
  # called in your application, but only called by #create_table.
  def new_table(name, field_defs, encrypt, record_class)
    # Header rec consists of last record no. used, delete count, and
    # all field names/types.  Here, I am inserting the 'recno' field
    # at the beginning of the fields.
    header_rec = ['000000', '000000', record_class, 'recno:Integer',
     field_defs].join('|')

    header_rec = 'Z' + encrypt_str(header_rec) if encrypt

    begin
      fptr = open(File.join(@db.path, name.to_s + @db.ext), 'w')
      fptr.write(header_rec + "\n")
    ensure
      fptr.close
    end
  end

  def delete_table(tablename)
    with_write_lock(tablename) do
      File.delete(File.join(@db.path, tablename.to_s + @db.ext))
      remove_indexes(tablename)
      remove_recno_index(tablename)
      return true
    end
  end

  def get_total_recs(table)
    get_recs(table).size
  end

  def reset_recno_ctr(table)
    with_write_locked_table(table) do |fptr|
      encrypted, header_line = get_header_record(table, fptr)
      last_rec_no, rest_of_line = header_line.split('|', 2)
      write_header_record(table, fptr,
       ['%06d' % 0, rest_of_line].join('|'))
      return true
    end
  end

  def get_header_vars(table)
    with_table(table) do |fptr|
      encrypted, line = get_header_record(table, fptr)

      last_rec_no, del_ctr, record_class, *flds = line.split('|')
      field_names = flds.collect { |x| x.split(':')[0].to_sym }
      field_types = flds.collect { |x| x.split(':')[1].to_sym }
      field_indexes = [nil] * field_names.size
      field_defaults = [nil] * field_names.size
      field_requireds = [false] * field_names.size
      field_extras = [nil] * field_names.size

      flds.each_with_index do |x,i|
        field_extras[i] = {}
        if x.split(':').size > 2
          x.split(':')[2..-1].each do |y|
            if y =~ /Index/
              field_indexes[i] = y
            elsif y =~ /Default/
              field_defaults[i] = \
               convert_to_native_type(field_types[i],
                y.split('->')[1])
            elsif y =~ /Required/
              field_requireds[i] = \
               convert_to_native_type(:Boolean,
                y.split('->')[1])
            else
              field_extras[i][y.split('->')[0]] = \
               y.split('->')[1]
            end
          end
        end
      end
      return [encrypted, last_rec_no.to_i, del_ctr.to_i,
       record_class, field_names, field_types, field_indexes,
       field_defaults, field_requireds, field_extras]
    end
  end

  def get_recs(table)
    encrypted = table.encrypted?
    recs = []

    with_table(table) do |fptr|
      begin
        # Skip header rec.
        fptr.readline

        # Loop through table.
        while true
          # Record current position in table.
          fpos = fptr.tell
          rec, line_length = line_to_rec(fptr.readline, encrypted)

          next if rec.empty?

          rec << fpos << line_length
          recs << rec
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end
      return recs
    end
  end

  def get_recs_by_recno(table, recnos)
    encrypted = table.encrypted?
    recs = []
    recno_idx = get_recno_index(table)

    with_table(table) do |fptr|
      # Skip header rec.
      fptr.readline

      # Take all the recnos you want to get, add the file positions
      # to them, and sort by file position, so that when we seek
      # through the physical file we are going in ascending file
      # position order, which should be fastest.
      recnos.collect { |r| [recno_idx[r], r] }.sort.each do |r|
        fptr.seek(r[0])
        rec, line_length = line_to_rec(fptr.readline, encrypted)

        next if rec.empty?

        raise "Index Corrupt!" unless rec[0].to_i == r[1]
        rec << r[0] << line_length
        recs << rec
      end
      return recs
    end
  end

  def get_rec_by_recno(table, recno)
    encrypted = table.encrypted?
    recno_idx = get_recno_index(table)

    return nil unless recno_idx.has_key?(recno)

    with_table(table) do |fptr|
      fptr.seek(recno_idx[recno])
      rec, line_length = line_to_rec(fptr.readline, encrypted)

      raise "Recno Index Corrupt for table %s!" % table.name if rec.empty?

      raise "Recno Index Corrupt for table %s!" % table.name unless rec[0].to_i == recno

      rec << recno_idx[recno] << line_length
      return rec
    end
  end

  def line_to_rec(line, encrypted)
    line.chomp!
    line_length = line.size
    line = unencrypt_str(line) if encrypted
    line.strip!

    # Convert line to rec and return rec and line length.
    return line.split('|', -1), line_length
  end

  def insert_record(table, rec)
    with_write_locked_table(table) do |fptr|
      # Auto-increment the record number field.
      rec_no = incr_rec_no_ctr(table, fptr)

      # Insert the newly created record number value at the beginning
      # of the field values.
      rec[0] = rec_no

      fptr.seek(0, IO::SEEK_END)
      fpos = fptr.tell

      write_record(table, fptr, 'end', rec.join('|'))

      add_to_indexes(table, rec, fpos)

      # Return the record number of the newly created record.
      return rec_no
    end
  end

  def update_records(table, recs)
    with_write_locked_table(table) do |fptr|
      recs.each do |rec|
        line = rec[:rec].join('|')

        # This doesn't actually 'delete' the line, it just
        # makes it all spaces.  That way, if the updated
        # record is the same or less length than the old
        # record, we can write the record back into the
        # same spot.  If the updated record is greater than
        # the old record, we will leave the now spaced-out
        # line and write the updated record at the end of
        # the file.
        write_record(table, fptr, rec[:fpos],
         ' ' * rec[:line_length])
        if line.size > rec[:line_length]
          fptr.seek(0, IO::SEEK_END)
          new_fpos = fptr.tell
          write_record(table, fptr, 'end', line)
          incr_del_ctr(table, fptr)

          update_recno_index(table, rec[:rec].first, new_fpos)
        else
          write_record(table, fptr, rec[:fpos], line)
        end
        update_to_indexes(table, rec[:rec])
      end
      # Return the number of records updated.
      return recs.size
    end
  end

  def delete_records(table, recs)
    with_write_locked_table(table) do |fptr|
      recs.each do |rec|
        # Go to offset within the file where the record is and
        # replace it with all spaces.
        write_record(table, fptr, rec.fpos, ' ' * rec.line_length)
        incr_del_ctr(table, fptr)

        delete_from_indexes(table, rec, rec.fpos)
      end

      # Return the number of records deleted.
      return recs.size
    end
  end

  def change_column_type(table, col_name, col_type)
    col_index = table.field_names.index(col_name)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
      else
        header_rec = line.split('|')
      end

      temp_fields = header_rec[col_index+3].split(':')
      temp_fields[1] = col_type.to_s
      header_rec[col_index+3] = temp_fields.join(':')

      if line[0..0] == 'Z'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          new_fptr.write(fptr.readline)
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def rename_column(table, old_col_name, new_col_name)
    col_index = table.field_names.index(old_col_name)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
      else
        header_rec = line.split('|')
      end

      temp_fields = header_rec[col_index+3].split(':')
      temp_fields[0] = new_col_name.to_s
      header_rec[col_index+3] = temp_fields.join(':')

      if line[0..0] == 'Z'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          new_fptr.write(fptr.readline)
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def add_column(table, field_def, after)
    # Find the index position of where to insert the column, either at
    # the end (-1) or after the field specified.
    if after.nil? or table.field_names.last == after
      insert_after = -1
    else
      insert_after = table.field_names.index(after)+1
    end

    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
      else
        header_rec = line.split('|')
      end

      if insert_after == -1
        header_rec.insert(insert_after, field_def)
      else
        # Need to account for recno ctr, delete ctr, record class.
        header_rec.insert(insert_after+3, field_def)
      end

      if line[0..0] == 'Z'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          line = fptr.readline.chomp

          if table.encrypted?
            temp_line = unencrypt_str(line)
          else
            temp_line = line
          end

          rec = temp_line.split('|', -1)
          rec.insert(insert_after, '')

          if table.encrypted?
            new_fptr.write(encrypt_str(rec.join('|')) + "\n")
          else
            new_fptr.write(rec.join('|') + "\n")
          end
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def drop_column(table, col_name)
    col_index = table.field_names.index(col_name)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
        header_rec.delete_at(col_index+3)
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        header_rec = line.split('|')
        header_rec.delete_at(col_index+3)
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          line = fptr.readline.chomp

          if table.encrypted?
            temp_line = unencrypt_str(line)
          else
            temp_line = line
          end

          rec = temp_line.split('|', -1)
          rec.delete_at(col_index)

          if table.encrypted?
            new_fptr.write(encrypt_str(rec.join('|')) + "\n")
          else
            new_fptr.write(rec.join('|') + "\n")
          end
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def rename_table(old_tablename, new_tablename)
    old_full_path = File.join(@db.path, old_tablename.to_s + @db.ext)
    new_full_path = File.join(@db.path, new_tablename.to_s + @db.ext)
    File.rename(old_full_path, new_full_path)
  end

  def add_index(table, col_names, index_no)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
      else
        header_rec = line.split('|')
      end

      col_names.each do |c|
        header_rec[table.field_names.index(c)+3] += \
         ':Index->%d' % index_no
      end

      if line[0..0] == 'Z'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          new_fptr.write(fptr.readline)
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def drop_index(table, col_names)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
      else
        header_rec = line.split('|')
      end

      col_names.each do |c|
        temp_field_def = \
         header_rec[table.field_names.index(c)+3].split(':')
        temp_field_def = temp_field_def.delete_if {|x|
          x =~ /Index->/
        }
        header_rec[table.field_names.index(c)+3] = \
         temp_field_def.join(':')
      end

      if line[0..0] == 'Z'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          new_fptr.write(fptr.readline)
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def change_column_default_value(table, col_name, value)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
      else
        header_rec = line.split('|')
      end

      if header_rec[table.field_names.index(col_name)+3] =~ \
       /Default->/
        hr_chunks = \
         header_rec[table.field_names.index(col_name)+3].split(':')

        if value.nil?
          hr_chunks = hr_chunks.delete_if { |x| x =~ /Default->/ }
          header_rec[table.field_names.index(col_name)+3] = \
           hr_chunks.join(':')
        else
          hr_chunks.collect! do |x|
            if x =~ /Default->/
              'Default->%s' % value
            else
              x
            end
          end
          header_rec[table.field_names.index(col_name)+3] = \
           hr_chunks.join(':')
        end
      else
        if value.nil?
        else
          header_rec[table.field_names.index(col_name)+3] += \
           ':Default->%s' % value
        end
      end

      if line[0..0] == 'Z'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          new_fptr.write(fptr.readline)
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def change_column_required(table, col_name, required)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp

      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
      else
        header_rec = line.split('|')
      end

      if header_rec[table.field_names.index(col_name)+3
       ] =~ /Required->/
        hr_chunks = \
         header_rec[table.field_names.index(col_name)+3].split(':')
        if not required
          hr_chunks = hr_chunks.delete_if {|x| x =~ /Required->/}
          header_rec[table.field_names.index(col_name)+3] = \
           hr_chunks.join(':')
        else
          hr_chunks.collect! do |x|
            if x =~ /Required->/
              'Default->%s' % required
            else
              x
            end
          end
          header_rec[table.field_names.index(col_name)+3] = \
           hr_chunks.join(':')
        end
      else
        if not required
        else
          header_rec[table.field_names.index(col_name)+3] += \
           ':Required->%s' % required
        end
      end

      if line[0..0] == 'Z'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        new_fptr.write(header_rec.join('|') + "\n")
      end

      begin
        while true
          new_fptr.write(fptr.readline)
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)
    end
  end

  def pack_table(table)
    with_write_lock(table.name) do
      fptr = open(table.filename, 'r')
      new_fptr = open(table.filename+'temp', 'w')

      line = fptr.readline.chomp
      # Reset the delete counter in the header rec to 0.
      if line[0..0] == 'Z'
        header_rec = unencrypt_str(line[1..-1]).split('|')
        header_rec[1] = '000000'
        new_fptr.write('Z' + encrypt_str(header_rec.join('|')) +
         "\n")
      else
        header_rec = line.split('|')
        header_rec[1] = '000000'
        new_fptr.write(header_rec.join('|') + "\n")
      end

      lines_deleted = 0

      begin
        while true
          line = fptr.readline

          if table.encrypted?
            temp_line = unencrypt_str(line)
          else
            temp_line = line
          end

          if temp_line.strip == ''
            lines_deleted += 1
          else
            new_fptr.write(line)
          end
        end
      # Here's how we break out of the loop...
      rescue EOFError
      end

      # Close the table and release the write lock.
      fptr.close
      new_fptr.close
      File.delete(table.filename)
      FileUtils.mv(table.filename+'temp', table.filename)

      # Return the number of deleted records that were removed.
      return lines_deleted
    end
  end

  def read_memo_file(filepath)
    begin
      f = File.new(File.join(@db.memo_blob_path, filepath))
      return f.read
    ensure
      f.close
    end
  end

  def write_memo_file(filepath, contents)
    begin
      f = File.new(File.join(@db.memo_blob_path, filepath), 'w')
      f.write(contents)
    ensure
      f.close
    end
  end

  def read_blob_file(filepath)
    begin
      f = File.new(File.join(@db.memo_blob_path, filepath), 'rb')
      return f.read
    ensure
      f.close
    end
  end

  def write_blob_file(filepath, contents)
    begin
      f = File.new(File.join(@db.memo_blob_path, filepath), 'wb')
      f.write(contents)
    ensure
      f.close
    end
  end

  private

  def with_table(table, access='r')
    begin
      yield fptr = open(table.filename, access)
    ensure
      fptr.close
    end
  end

  def with_write_lock(tablename)
    begin
      write_lock(tablename) if @db.server?
      yield
    ensure
      write_unlock(tablename) if @db.server?
    end
  end

  def with_write_locked_table(table, access='r+')
    begin
      write_lock(table.name) if @db.server?
      yield fptr = open(table.filename, access)
    ensure
      fptr.close
      write_unlock(table.name) if @db.server?
    end
  end

  def write_lock(tablename)
    # Unless an key already exists in the hash holding mutex records
    # for this table, create a write key for this table in the mutex
    # hash.  Then, place a lock on that mutex.
    @mutex_hash[tablename] = Mutex.new unless (
     @mutex_hash.has_key?(tablename))
    @mutex_hash[tablename].lock

    return true
  end

  def write_unlock(tablename)
    # Unlock the write mutex for this table.
    @mutex_hash[tablename].unlock

    return true
  end

  def write_record(table, fptr, pos, record)
    if table.encrypted?
      temp_rec = encrypt_str(record)
    else
      temp_rec = record
    end

    # If record is to be appended, go to end of table and write
    # record, adding newline character.
    if pos == 'end'
      fptr.seek(0, IO::SEEK_END)
      fptr.write(temp_rec + "\n")
    else
      # Otherwise, overwrite another record (that's why we don't
      # add the newline character).
      fptr.seek(pos)
      fptr.write(temp_rec)
    end
  end

  def write_header_record(table, fptr, record)
    fptr.seek(0)

    if table.encrypted?
      fptr.write('Z' + encrypt_str(record) + "\n")
    else
      fptr.write(record + "\n")
    end
  end

  def get_header_record(table, fptr)
    fptr.seek(0)

    line = fptr.readline.chomp

    if line[0..0] == 'Z'
      [true, unencrypt_str(line[1..-1])]
    else
      [false, line]
    end
  end

  def incr_rec_no_ctr(table, fptr)
    encrypted, header_line = get_header_record(table, fptr)
    last_rec_no, rest_of_line = header_line.split('|', 2)
    last_rec_no = last_rec_no.to_i + 1

    write_header_record(table, fptr, ['%06d' % last_rec_no,
     rest_of_line].join('|'))

    # Return the new recno.
    last_rec_no
  end

  def incr_del_ctr(table, fptr)
    encrypted, header_line = get_header_record(table, fptr)
    last_rec_no, del_ctr, rest_of_line = header_line.split('|', 3)
    del_ctr = del_ctr.to_i + 1

    write_header_record(table, fptr, [last_rec_no, '%06d' % del_ctr,
     rest_of_line].join('|'))

    true
  end

end