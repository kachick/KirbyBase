class KBIndex

  include KBTypeConversionsMixin
  include KBEncryptionMixin

  def initialize(table, index_fields)
    @last_update = Time.new
    @idx_arr = []
    @table = table
    @index_fields = index_fields
    @col_poss = index_fields.collect {|i| table.field_names.index(i) }
    @col_names = index_fields
    @col_types = index_fields.collect {|i|
     table.field_types[table.field_names.index(i)]}
  end

  def get_idx
    @idx_arr
  end

  def get_timestamp
    @last_update
  end

  def rebuild(fptr)
    @idx_arr.clear

    encrypted = @table.encrypted?

    # Skip header rec.
    fptr.readline

    begin
      # Loop through table.
      while true
        line = fptr.readline

        line = unencrypt_str(line) if encrypted
        line.strip!

        # If blank line (i.e. 'deleted'), skip it.
        next if line == ''

        # Split the line up into fields.
        rec = line.split('|', @col_poss.max+2)

        append_new_rec_to_index_array(rec)
      end
    # Here's how we break out of the loop...
    rescue EOFError
    end

    @last_update = Time.new
  end

  def add_index_rec(rec)
    @last_upddate = Time.new if append_new_rec_to_index_array(rec)
  end

  def delete_index_rec(recno)
    i = @idx_arr.rassoc(recno.to_i)
    @idx_arr.delete_at(@idx_arr.index(i)) unless i.nil?
    @last_update = Time.new
  end

  def update_index_rec(rec)
    delete_index_rec(rec.first.to_i)
    add_index_rec(rec)
  end

  def append_new_rec_to_index_array(rec)
    idx_rec = []
    @col_poss.zip(@col_types).each do |col_pos, col_type|
      idx_rec << convert_to_native_type(col_type, rec[col_pos])
     end

    return false if idx_rec.uniq == [kb_nil]

    idx_rec << rec.first.to_i
    @idx_arr << idx_rec
    true
  end

end