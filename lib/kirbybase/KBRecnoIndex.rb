class KBRecnoIndex

  include KBEncryptionMixin

  def initialize(table)
    @idx_hash = {}
    @table = table
  end

  def get_idx
    return @idx_hash
  end

  def rebuild(fptr)
    @idx_hash.clear

    encrypted = @table.encrypted?

    begin
      # Skip header rec.
      fptr.readline

      # Loop through table.
      while true
        # Record current position in table.  Then read first
        # detail record.
        fpos = fptr.tell
        line = fptr.readline

        line = unencrypt_str(line) if encrypted
        line.strip!

        # If blank line (i.e. 'deleted'), skip it.
        next if line == ''

        # Split the line up into fields.
        rec = line.split('|', 2)

        @idx_hash[rec.first.to_i] = fpos
      end
    # Here's how we break out of the loop...
    rescue EOFError
    end
  end

  def add_index_rec(recno, fpos)
    raise 'Table already has index record for recno: %s' % recno if @idx_hash.has_key?(recno.to_i)
    @idx_hash[recno.to_i] = fpos
  end

  def update_index_rec(recno, fpos)
    raise 'Table has no index record for recno: %s' % recno unless @idx_hash.has_key?(recno.to_i)
    @idx_hash[recno.to_i] = fpos
  end

  def delete_index_rec(recno)
    raise 'Table has no index record for recno: %s' % recno unless @idx_hash.has_key?(recno.to_i)
    @idx_hash.delete(recno.to_i)
  end

end