class KBTable

  include DRb::DRbUndumped
  include KBTypeConversionsMixin

  # Make constructor private.  KBTable instances should only be created
  # from KirbyBase#get_table.
  private_class_method :new

  VALID_FIELD_TYPES = [:String, :Integer, :Float, :Boolean, :Date, :Time,
             :DateTime, :Memo, :Blob, :ResultSet, :YAML]
  VALID_DEFAULT_TYPES = [:String, :Integer, :Float, :Boolean, :Date,
               :Time, :DateTime, :YAML]
  VALID_INDEX_TYPES = [:String, :Integer, :Float, :Boolean, :Date, :Time,
             :DateTime]

  attr_reader :filename, :name, :table_class, :db, :lookup_key, \
               :last_rec_no, :del_ctr

  # Return true if valid field type.
  #
  # *field_type*:: Symbol specifying field type.
  def KBTable.valid_field_type?(field_type)
    VALID_FIELD_TYPES.include?(field_type)
  end

  # Return true if data is correct type, false otherwise.
  #
  # *data_type*:: Symbol specifying data type.
  # *value*:: Value to convert to String.
  def KBTable.valid_data_type?(data_type, value)
    case data_type
    when /:String|:Blob/
      return false unless value.respond_to?(:to_str)
    when :Memo
      return false unless value.is_a?(KBMemo)
    when :Blob
      return false unless value.is_a?(KBBlob)
    when :Boolean
      return false unless value.is_a?(TrueClass) or value.is_a?(
       FalseClass)
    when :Integer
      return false unless value.respond_to?(:to_int)
    when :Float
      return false unless value.respond_to?(:to_f)
    when :Time
      return false unless value.is_a?(Time)
    when :Date
      return false unless value.is_a?(Date)
    when :DateTime
      return false unless value.is_a?(DateTime)
    when :YAML
      return false unless value.respond_to?(:to_yaml)
    end

    return true
  end

  # Return true if valid default type.
  #
  # *field_type*:: Symbol specifying field type.
  def KBTable.valid_default_type?(field_type)
    VALID_DEFAULT_TYPES.include?(field_type)
  end

  # Return true if valid index type.
  #
  # *field_type*:: Symbol specifying field type.
  def KBTable.valid_index_type?(field_type)
    VALID_INDEX_TYPES.include?(field_type)
  end

  # Return a new instance of KBTable.  Should never be called directly by
  # your application.  Should only be called from KirbyBase#get_table.
  def KBTable.create_called_from_database_instance(db, name, filename)
    return new(db, name, filename)
  end

  # This has been declared private so user's cannot create new instances
  # of KBTable from their application.  A user gets a handle to a KBTable
  # instance by calling KirbyBase#get_table for an existing table or
  # KirbyBase.create_table for a new table.
  def initialize(db, name, filename)
    @db = db
    @name = name
    @filename = filename
    @encrypted = false
    @lookup_key = :recno
    @idx_timestamps = {}
    @idx_arrs = {}

    # Alias delete_all to clear method.
    alias delete_all clear

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Returns true if table is encrypted.
  def encrypted?
    if @encrypted
      return true
    else
      return false
    end
  end

  # Return array containing table field names.
  def field_names
    return @field_names
  end

  # Return array containing table field types.
  def field_types
    return @field_types
  end

  # Return array containing table field extras.
  def field_extras
    return @field_extras
  end

  # Return array containing table field indexes.
  def field_indexes
    return @field_indexes
  end

  # Return array containing table field defaults.
  def field_defaults
    return @field_defaults
  end

  # Return array containing table field requireds.
  def field_requireds
    return @field_requireds
  end

  # Insert a new record into a table, return unique record number.
  #
  # *data*:: Array, Hash, Struct instance containing field values of
  #      new record.
  # *insert_proc*:: Proc instance containing insert code. This and the
  #         data parameter are mutually exclusive.
  def insert(*data, &insert_proc)
    raise 'Cannot specify both a hash/array/struct and a ' + \
     'proc for method #insert!' unless data.empty? or insert_proc.nil?

    raise 'Must specify either hash/array/struct or insert ' + \
     'proc for method #insert!' if data.empty? and insert_proc.nil?

    # Update the header variables.
    update_header_vars

    # Convert input, which could be a proc, an array, a hash, or a
    # Struct into a common format (i.e. hash).
    if data.empty?
      input_rec = convert_input_data(insert_proc)
    else
      input_rec = convert_input_data(data)
    end

    # Check the field values to make sure they are proper types.
    validate_input(input_rec)

    input_rec = Struct.new(*field_names).new(*field_names.zip(
     @field_defaults).collect do |fn, fd|
      if input_rec.has_key?(fn)
        input_rec[fn]
      else
        fd
      end
    end)

    check_required_fields(input_rec)

    check_against_input_for_specials(input_rec)

    new_recno = @db.engine.insert_record(self, @field_names.zip(
     @field_types).collect do |fn, ft|
      convert_to_encoded_string(ft, input_rec[fn])
    end)

    # If there are any associated memo/blob fields, save their values.
    input_rec.each { |r| r.write_to_file if r.is_a?(KBMemo) } if \
     @field_types.include?(:Memo)
    input_rec.each { |r| r.write_to_file if r.is_a?(KBBlob) } if \
     @field_types.include?(:Blob)

    return new_recno
  end

  # Return array of records (Structs) to be updated, in this case all
  # records.
  #
  # *updates*:: Hash or Struct containing updates.
  def update_all(*updates, &update_proc)
    raise 'Cannot specify both a hash/array/struct and a ' + \
     'proc for method #update_all!' unless updates.empty? or \
     update_proc.nil?

    raise 'Must specify either hash/array/struct or update ' + \
     'proc for method #update_all!' if updates.empty? and \
     update_proc.nil?

    # Depending on whether the user supplied an array/hash/struct or a
    # block as update criteria, we are going to call updates in one of
    # two ways.
    if updates.empty?
      update { true }.set &update_proc
    else
      update(*updates) { true }
    end
  end

  # Return array of records (Structs) to be updated based on select cond.
  #
  # *updates*:: Hash or Struct containing updates.
  # *select_cond*:: Proc containing code to select records to update.
  def update(*updates, &select_cond)
    raise ArgumentError, "Must specify select condition code " + \
     "block.  To update all records, use #update_all instead." if \
     select_cond.nil?

    # Update the header variables.
    update_header_vars

    # Get all records that match the selection criteria and
    # return them in an array.
    result_set = get_matches(:update, @field_names, select_cond)

    # If updates is empty, this means that the user must have specified
    # the updates in KBResultSet#set, i.e.
    # tbl.update {|r| r.recno == 1}.set(:name => 'Bob')
    return result_set if updates.empty?

    # Call KBTable#set and pass it the records to be updated and the
    # updated criteria.
    set(result_set, updates)
  end

  # Update record whose recno field equals index.
  #
  # *index*:: Integer specifying recno you wish to select.
  # *updates*:: Hash, Struct, or Array containing updates.
  def []=(index, updates)
    return update(updates) { |r| r.recno == index }
  end

  # Set fields of records to updated values.  Returns number of records
  # updated.
  #
  # *recs*:: Array of records (Structs) that will be updated.
  # *data*:: Hash, Struct, Proc containing updates.
  def set(recs, data)
    # If updates are not in the form of a Proc, convert updates, which
    # could be an array, a hash, or a Struct into a common format (i.e.
    # hash).
    update_rec = convert_input_data(data) unless data.is_a?(Proc)

    updated_recs = []

    # For each one of the recs that matched the update query, apply the
    # updates to it and write it back to the database table.
    recs.each do |rec|
      temp_rec = rec.dup

      if data.is_a?(Proc)
        begin
          data.call(temp_rec)
        rescue NoMethodError
          raise 'Invalid field name in code block: %s' % $!
        end
       else
        @field_names.each { |fn| temp_rec[fn] = update_rec.fetch(fn,
         temp_rec.send(fn)) }
      end

      # Is the user trying to change something they shouldn't?
      raise 'Cannot update recno field!' unless \
       rec.recno == temp_rec.recno
      raise 'Cannot update internal fpos field!' unless \
       rec.fpos == temp_rec.fpos
      raise 'Cannot update internal line_length field!' unless \
       rec.line_length == temp_rec.line_length

      # Are the data types of the updates correct?
      validate_input(temp_rec)

      check_required_fields(temp_rec)

      check_against_input_for_specials(temp_rec)

      # Apply updates to the record and add it to an array holding
      # updated records.  We need the fpos and line_length because
      # the engine will use them to determine where to write the
      # update and whether the updated record will fit in the old
      # record's spot.
      updated_recs << { :rec => @field_names.zip(@field_types
       ).collect { |fn, ft| convert_to_encoded_string(ft,
       temp_rec.send(fn)) }, :fpos => rec.fpos,
       :line_length => rec.line_length }


      # Update any associated blob/memo fields.
      temp_rec.each { |r| r.write_to_file if r.is_a?(KBMemo) } if \
       @field_types.include?(:Memo)
      temp_rec.each { |r| r.write_to_file if r.is_a?(KBBlob) } if \
       @field_types.include?(:Blob)
    end

    # Take all of the update records and write them back out to the
    # table's file.
    @db.engine.update_records(self, updated_recs)

    # Return the number of records updated.
    recs.size
  end

  # Delete records from table and return # deleted.
  #
  # *select_cond*:: Proc containing code to select records.
  def delete(&select_cond)
    raise ArgumentError, 'Must specify select condition code ' + \
     'block.  To delete all records, use #clear instead.' if \
     select_cond.nil?

    # Get all records that match the selection criteria and
    # return them in an array.
    result_set = get_matches(:delete, [:recno], select_cond)

    @db.engine.delete_records(self, result_set)

    # Return the number of records deleted.
    result_set.size
  end

  # Delete all records from table. You can also use #delete_all.
  #
  # *reset_recno_ctr*:: true/false specifying whether recno counter should
  #           be reset to 0.
  def clear(reset_recno_ctr=true)
    recs_deleted = delete { true }
    pack

    @db.engine.reset_recno_ctr(self) if reset_recno_ctr
    update_header_vars
    recs_deleted
  end

  # Return the record(s) whose recno field is included in index.
  #
  # *index*:: Array of Integer(s) specifying recno(s) you wish to select.
  def [](*index)
    return nil if index[0].nil?

    return get_match_by_recno(:select, @field_names, index[0]) if \
     index.size == 1

    recs = select_by_recno_index(*@field_names) { |r|
      index.include?(r.recno)
    }

    recs
  end

  # Return array of records (Structs) matching select conditions.
  #
  # *filter*:: List of field names (Symbols) to include in result set.
  # *select_cond*:: Proc containing select code.
  #
  def select(*filter, &select_cond)
    # Declare these variables before the code block so they don't go
    # after the code block is done.
    result_set = []

    # Validate that all names in filter are valid field names.
    validate_filter(filter)

    filter = @field_names if filter.empty?

    # Get all records that match the selection criteria and
    # return them in an array of Struct instances.
    get_matches(:select, filter, select_cond)
  end

  # Return array of records (Structs) matching select conditions.  Select
  # condition block should not contain references to any table column
  # except :recno.  If you need to select by other table columns than just
  # :recno, use #select instead.
  #
  # *filter*:: List of field names (Symbols) to include in result set.
  # *select_cond*:: Proc containing select code.
  #
  def select_by_recno_index(*filter, &select_cond)
    # Declare these variables before the code block so they don't go
    # after the code block is done.
    result_set = []

    # Validate that all names in filter are valid field names.
    validate_filter(filter)

    filter = @field_names if filter.empty?

    # Get all records that match the selection criteria and
    # return them in an array of Struct instances.
    get_matches_by_recno_index(:select, filter, select_cond)
  end

  # Remove blank records from table, return total removed.
  def pack
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    lines_deleted = @db.engine.pack_table(self)

    update_header_vars

    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)
    create_indexes
    create_table_class unless @db.server?

    return lines_deleted
  end

  # Rename a column.
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *old_col_name*:: Symbol of old column name.
  # *new_col_name*:: Symbol of new column name.
  def rename_column(old_col_name, new_col_name)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    raise "Cannot rename recno column!" if old_col_name == :recno
    raise "Cannot give column name of recno!" if new_col_name == :recno

     raise 'Invalid column name to rename: ' % old_col_name unless \
     @field_names.include?(old_col_name)

     raise 'New column name already exists: ' % new_col_name if \
     @field_names.include?(new_col_name)

    @db.engine.rename_column(self, old_col_name, new_col_name)

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Change a column's type.
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *col_name*:: Symbol of column name.
  # *col_type*:: Symbol of new column type.
  def change_column_type(col_name, col_type)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    raise "Cannot change type for recno column!" if col_name == :recno
    raise 'Invalid column name: ' % col_name unless \
     @field_names.include?(col_name)

    raise 'Invalid field type: %s' % col_type unless \
     KBTable.valid_field_type?(col_type)

    @db.engine.change_column_type(self, col_name, col_type)

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Add a column to table.
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *col_name*:: Symbol of column name to add.
  # *col_type*:: Symbol (or Hash if includes field extras) of column type
  #        to add.
  # *after*:: Symbol of column name that you want to add this column
  #       after.
  def add_column(col_name, col_type, after=nil)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    raise "Invalid column name in 'after': #{after}" unless after.nil? \
     or @field_names.include?(after)

    raise "Invalid column name in 'after': #{after}" if after == :recno

    raise "Column name cannot be recno!" if col_name == :recno

    raise "Column name already exists!" if @field_names.include?(
     col_name)

    # Does this new column have field extras (i.e. Index, Lookup, etc.)
    if col_type.is_a?(Hash)
      temp_type = col_type[:DataType]
    else
      temp_type = col_type
    end

    raise 'Invalid field type: %s' % temp_type unless \
     KBTable.valid_field_type?(temp_type)

    field_def = @db.build_header_field_string(col_name, col_type)

    @db.engine.add_column(self, field_def, after)

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Drop a column from table.
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *col_name*:: Symbol of column name to add.
  def drop_column(col_name)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    raise 'Invalid column name: ' % col_name unless \
     @field_names.include?(col_name)

    raise "Cannot drop :recno column!" if col_name == :recno

    @db.engine.drop_column(self, col_name)

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Add an index to a column.
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *col_names*:: Array containing column name(s) of new index.
  def add_index(*col_names)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    col_names.each do |c|
      raise "Invalid column name: #{c}" unless \
       @field_names.include?(c)

      raise "recno column cannot be indexed!" if c == :recno

      raise "Column already indexed: #{c}" unless \
       @field_indexes[@field_names.index(c)].nil?
    end

    last_index_no_used = 0
    @field_indexes.each do |i|
      next if i.nil?
      index_no = i[-1..-1].to_i
      last_index_no_used = index_no if index_no > last_index_no_used
    end

    @db.engine.add_index(self, col_names, last_index_no_used+1)

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Drop an index on a column(s).
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *col_names*:: Array containing column name(s) of new index.
  def drop_index(*col_names)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    col_names.each do |c|
      raise "Invalid column name: #{c}" unless \
       @field_names.include?(c)

      raise "recno column index cannot be dropped!" if c == :recno

      raise "Column not indexed: #{c}" if \
       @field_indexes[@field_names.index(c)].nil?
    end

    @db.engine.drop_index(self, col_names)

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Change a column's default value.
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *col_name*:: Symbol of column name.
  # *value*:: New default value for column.
  def change_column_default_value(col_name, value)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    raise ":recno cannot have a default value!" if col_name == :recno

    raise 'Invalid column name: ' % col_name unless \
     @field_names.include?(col_name)

    raise 'Cannot set default value for this type: ' + \
     '%s' % @field_types.index(col_name) unless \
     KBTable.valid_default_type?(
      @field_types[@field_names.index(col_name)])

    if value.nil?
      @db.engine.change_column_default_value(self, col_name, nil)
    else
      @db.engine.change_column_default_value(self, col_name,
       convert_to_encoded_string(
        @field_types[@field_names.index(col_name)], value))
    end

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Change whether a column is required.
  #
  # Make sure you are executing this method while in single-user mode
  # (i.e. not running in client/server mode).
  #
  # *col_name*:: Symbol of column name.
  # *required*:: true or false.
  def change_column_required(col_name, required)
    raise "Do not execute this method in client/server mode!" if \
     @db.client?

    raise ":recno is always required!" if col_name == :recno

    raise 'Invalid column name: ' % col_name unless \
     @field_names.include?(col_name)

    raise 'Required must be either true or false!' unless \
     [true, false].include?(required)

    @db.engine.change_column_required(self, col_name, required)

    # Need to reinitialize the table instance and associated indexes.
    @db.engine.remove_recno_index(@name)
    @db.engine.remove_indexes(@name)

    update_header_vars
    create_indexes
    create_table_class unless @db.server?
  end

  # Return total number of undeleted (blank) records in table.
  def total_recs
    return @db.engine.get_total_recs(self)
  end

  # Import csv file into table.
  #
  # *csv_filename*:: filename of csv file to import.
  def import_csv(csv_filename)
    records_inserted = 0
    tbl_rec = @table_class.new(self)

    # read with FasterCSV if loaded, or the standard CSV otherwise
    (defined?(FasterCSV) ? FasterCSV : CSV).foreach(csv_filename
     ) do |row|
      tbl_rec.populate([nil] + row)
      insert(tbl_rec)
      records_inserted += 1
    end
    return records_inserted
  end

  private

  def create_indexes
    # First remove any existing select_by_index methods.  This is in
    # case we are dropping an index or a column.  We want to make sure
    # an select_by_index method doesn't hang around if it's index or
    # column has been dropped.
    methods.each do |m|
      next if m.to_s == 'select_by_recno_index'

      if m =~ /select_by_.*_index/
        class << self; self end.send(:remove_method, m.to_sym)
      end
    end

    # Create the recno index.  A recno index always gets created even if
    # there are no user-defined indexes for the table.
    @db.engine.init_recno_index(self)

    # There can be up to 5 different indexes on a table.  Any of these
    # indexes can be single or compound.
    ['Index->1', 'Index->2', 'Index->3', 'Index->4',
     'Index->5'].each do |idx|
      index_col_names = []
      @field_indexes.each_with_index do |fi,i|
        next if fi.nil?
        index_col_names << @field_names[i] if fi.include?(idx)
      end

      # If no fields were indexed on this number (1..5), go to the
      # next index number.
      next if index_col_names.empty?

      # Create this index on the engine.
      @db.engine.init_index(self, index_col_names)

      # For each index found, add an instance method for it so that
      # it can be used for #selects.
      select_meth_str = <<-END_OF_STRING
      def select_by_#{index_col_names.join('_')}_index(*filter,
       &select_cond)
        result_set = []
        validate_filter(filter)
        filter = @field_names if filter.empty?
        return get_matches_by_index(:select,
         [:#{index_col_names.join(',:')}], filter, select_cond)
      end
      END_OF_STRING

      instance_eval(select_meth_str) unless @db.server?

      @idx_timestamps[index_col_names.join('_')] = nil
      @idx_arrs[index_col_names.join('_')] = nil
    end
  end

  def create_table_class
    #This is the class that will be used in #select condition blocks.
    @table_class = Class.new(KBTableRec)

    get_meth_str = ''
    get_meth_upd_res_str = ''
    set_meth_str = ''

    @field_names.zip(@field_types, @field_extras) do |x|
      field_name, field_type, field_extra = x

      @lookup_key = field_name if field_extra.has_key?('Key')

      # These are the default get/set methods for the table column.
      get_meth_str = <<-END_OF_STRING
      def #{field_name}
        return @#{field_name}
      end
      END_OF_STRING
      get_meth_upd_res_str = <<-END_OF_STRING
      def #{field_name}_upd_res
        return @#{field_name}
      end
      END_OF_STRING
      set_meth_str = <<-END_OF_STRING
      def #{field_name}=(s)
        @#{field_name} = convert_to_native_type(:#{field_type}, s)
      end
      END_OF_STRING

      # If this is a Lookup field, modify the get_method.
      if field_extra.has_key?('Lookup')
        lookup_table, key_field = field_extra['Lookup'].split('.')

        # If joining to recno field of lookup table use the
        # KBTable[] method to get the record from the lookup table.
        if key_field == 'recno'
          get_meth_str = <<-END_OF_STRING
          def #{field_name}
            table = @tbl.db.get_table(:#{lookup_table})
            return table[@#{field_name}]
          end
          END_OF_STRING
        else
          begin
            unless @db.get_table(lookup_table.to_sym
             ).respond_to?('select_by_%s_index' % key_field)
              raise RuntimeError
            end

            get_meth_str = <<-END_OF_STRING
            def #{field_name}
              table = @tbl.db.get_table(:#{lookup_table})
              return table.select_by_#{key_field}_index { |r|
               r.#{key_field} == @#{field_name} }[0]
            end
            END_OF_STRING
          rescue RuntimeError
            get_meth_str = <<-END_OF_STRING
            def #{field_name}
              table = @tbl.db.get_table(:#{lookup_table})
              return table.select { |r|
               r.#{key_field} == @#{field_name} }[0]
            end
            END_OF_STRING
          end
        end
      end

      # If this is a Link_many field, modify the get/set methods.
      if field_extra.has_key?('Link_many')
        lookup_field, rest = field_extra['Link_many'].split('=')
        link_table, link_field = rest.split('.')

        begin
          unless @db.get_table(link_table.to_sym).respond_to?(
           'select_by_%s_index' % link_field)
            raise RuntimeError
          end

          get_meth_str = <<-END_OF_STRING
          def #{field_name}
            table = @tbl.db.get_table(:#{link_table})
            return table.select_by_#{link_field}_index { |r|
             r.send(:#{link_field}) == @#{lookup_field} }
          end
          END_OF_STRING
        rescue RuntimeError
          get_meth_str = <<-END_OF_STRING
          def #{field_name}
            table = @tbl.db.get_table(:#{link_table})
            return table.select { |r|
             r.send(:#{link_field}) == @#{lookup_field} }
          end
          END_OF_STRING
        end

        get_meth_upd_res_str = <<-END_OF_STRING
        def #{field_name}_upd_res
          return kb_nil
        end
        END_OF_STRING
        set_meth_str = <<-END_OF_STRING
        def #{field_name}=(s)
          @#{field_name} = kb_nil
        end
        END_OF_STRING
      end

      # If this is a Calculated field, modify the get/set methods.
      if field_extra.has_key?('Calculated')
        calculation = field_extra['Calculated']

        get_meth_str = <<-END_OF_STRING
        def #{field_name}()
          return #{calculation}
        end
        END_OF_STRING
        get_meth_upd_res_str = <<-END_OF_STRING
        def #{field_name}_upd_res()
          return kb_nil
        end
        END_OF_STRING
        set_meth_str = <<-END_OF_STRING
        def #{field_name}=(s)
          @#{field_name} = kb_nil
        end
        END_OF_STRING
      end

      @table_class.class_eval(get_meth_str)
      @table_class.class_eval(get_meth_upd_res_str)
      @table_class.class_eval(set_meth_str)
    end
  end

  # Check that filter contains valid field names.
  def validate_filter(filter)
    # Each field in the filter array must be a valid fieldname in the
    # table.
    filter.each { |f|
      raise 'Invalid field name: %s in filter!' % f unless \
       @field_names.include?(f)
    }
  end

  # Convert data passed to #input, #update, or #set to a common format.
  def convert_input_data(values)
    temp_hash = {}

    # This only applies to Procs in #insert, Procs in #update are
    # handled in #set.
    if values.is_a?(Proc)
      tbl_rec = Struct.new(*@field_names[1..-1]).new
      begin
        values.call(tbl_rec)
      rescue NoMethodError
        raise 'Invalid field name in code block: %s' % $!
      end

      @field_names[1..-1].each do |f|
        temp_hash[f] = tbl_rec[f] unless tbl_rec[f].nil?
      end

    # Is input data an instance of custom record class, Struct, or
    # KBTableRec?
    elsif values.first.is_a?(Object.full_const_get(@record_class)) or \
     values.first.is_a?(Struct) or values.first.class == @table_class
      @field_names[1..-1].each do |f|
        temp_hash[f] = values.first.send(f) if \
         values.first.respond_to?(f)
      end

    # Is input data a hash?
    elsif values.first.is_a?(Hash)
      temp_hash = values.first.dup

    # Is input data an array?
    elsif values.is_a?(Array)
      raise ArgumentError, 'Must specify all fields in input array!' \
       unless values.size == @field_names[1..-1].size

      @field_names[1..-1].each do |f|
        temp_hash[f] = values[@field_names.index(f)-1]
      end
    else
      raise(ArgumentError, 'Invalid type for values container!')
    end

    return temp_hash
  end

  # Check that all required fields have values.
  def check_required_fields(data)
    @field_names[1..-1].each do |f|
      raise(ArgumentError,
       'A value for this field is required: %s' % f) if \
       @field_requireds[@field_names.index(f)] and data[f].nil?
    end
  end

  # Check that no special field types (i.e. calculated or link_many
  # fields)
  # have been given values.
  def check_against_input_for_specials(data)
    @field_names[1..-1].each do |f|
      raise(ArgumentError,
       'You cannot input a value for this field: %s' % f) if \
       @field_extras[@field_names.index(f)].has_key?('Calculated') \
       or @field_extras[@field_names.index(f)].has_key?('Link_many') \
        and not data[f].nil?
    end
  end

  # Check input data to ensure proper data types.
  def validate_input(data)
    @field_names[1..-1].each do |f|
      next if data[f].nil?

      raise 'Invalid data %s for column %s' % [data[f], f] unless \
       KBTable.valid_data_type?(@field_types[@field_names.index(f)],
       data[f])
    end
  end

  # Read header record and update instance variables.
  def update_header_vars
    @encrypted, @last_rec_no, @del_ctr, @record_class, @col_names, \
     @col_types, @col_indexes, @col_defaults, @col_requireds, \
     @col_extras = @db.engine.get_header_vars(self)

    # These are deprecated.
    @field_names = @col_names
    @field_types = @col_types
    @field_indexes = @col_indexes
    @field_defaults = @col_defaults
    @field_requireds = @col_requireds
    @field_extras = @col_extras
  end

  # Return Struct object that will hold result record.
  def get_result_struct(query_type, filter)
    case query_type
    when :select
      return Struct.new(*filter) if @record_class == 'Struct'
    when :update
      return Struct.new(*(filter + [:fpos, :line_length]))
    when :delete
      return Struct.new(:recno, :fpos, :line_length)
    end
    return nil
  end

  # Return Struct/custom class populated with table row data.
  def create_result_rec(query_type, filter, result_struct, tbl_rec, rec)
    # If this isn't a select query or if it is a select query, but
    # the table record class is simply a Struct, then we will use
    # a Struct for the result record type.
    if query_type != :select
      result_rec = result_struct.new(*filter.collect { |f|
       tbl_rec.send("#{f}_upd_res".to_sym) })
    elsif @record_class == 'Struct'
      result_rec = result_struct.new(*filter.collect do |f|
        if tbl_rec.send(f).kb_nil?
          nil
        else
          tbl_rec.send(f)
        end
      end)
    else
      if Object.full_const_get(@record_class).respond_to?(:kb_create)
        result_rec = Object.full_const_get(@record_class
         ).kb_create(*@field_names.collect do |f|
          # Just a warning here:  If you specify a filter on
          # a select, you are only going to get those fields
          # you specified in the result set, EVEN IF
          # record_class is a custom class instead of Struct.
          if filter.include?(f)
            if tbl_rec.send(f).kb_nil?
              nil
            else
              tbl_rec.send(f)
            end
          else
            nil
          end
        end)
      elsif Object.full_const_get(@record_class).respond_to?(
       :kb_defaults)
        result_rec = Object.full_const_get(@record_class).new(
         *@field_names.collect do |f|
          if tbl_rec.send(f).kb_nil?
            nil
          else
            tbl_rec.send(f) || Object.full_const_get(
             @record_class).kb_defaults[@field_names.index(f)]
          end
        end)
      else
        result_rec = Object.full_const_get(@record_class).allocate
        @field_names.each do |fn|
          if tbl_rec.send(fn).kb_nil?
            result_rec.send("#{fn}=", nil)
          else
            result_rec.send("#{fn}=", tbl_rec.send(fn))
          end
        end
      end
    end

    unless query_type == :select
      result_rec.fpos = rec[-2]
      result_rec.line_length = rec[-1]
    end
    result_rec
  end

  # Return records from table that match select condition.
  def get_matches(query_type, filter, select_cond)
    result_struct = get_result_struct(query_type, filter)
    match_array = KBResultSet.new(self, filter, filter.collect { |f|
     @field_types[@field_names.index(f)] })

    tbl_rec = @table_class.new(self)

    # Loop through table.
    @db.engine.get_recs(self).each do |rec|
      tbl_rec.populate(rec)

      next if select_cond and not select_cond.call(tbl_rec)

      match_array << create_result_rec(query_type, filter,
       result_struct, tbl_rec, rec)
    end
    return match_array
  end

  # Return records from table that match select condition using one of
  # the table's indexes instead of searching the whole file.
  def get_matches_by_index(query_type, index_fields, filter, select_cond)
    good_matches = []

    idx_struct = Struct.new(*(index_fields + [:recno]))

    begin
      if @db.client?
        # If client, check to see if the copy of the index we have
        # is up-to-date.  If it is not up-to-date, grab a new copy
        # of the index array from the engine.
        unless @idx_timestamps[index_fields.join('_')] == \
         @db.engine.get_index_timestamp(self, index_fields.join(
         '_'))
          @idx_timestamps[index_fields.join('_')] = \
           @db.engine.get_index_timestamp(self, index_fields.join(
           '_'))

          @idx_arrs[index_fields.join('_')] = \
           @db.engine.get_index(self, index_fields.join('_'))
        end
      else
        # If running single-user, grab the index array from the
        # engine.
        @idx_arrs[index_fields.join('_')] = \
         @db.engine.get_index(self, index_fields.join('_'))
      end

      @idx_arrs[index_fields.join('_')].each do |rec|
        good_matches << rec[-1] if select_cond.call(
         idx_struct.new(*rec))
      end
    rescue NoMethodError
      raise 'Field name in select block not part of index!'
    end

    return get_matches_by_recno(query_type, filter, good_matches)
  end

  # Return records from table that match select condition using the
  # table's recno index instead of searching the whole file.
  def get_matches_by_recno_index(query_type, filter, select_cond)
    good_matches = []
    idx_struct = Struct.new(:recno)

    begin
      @db.engine.get_recno_index(self).each_key do |key|
        good_matches << key if select_cond.call(idx_struct.new(key))
      end
    rescue NoMethodError
      raise "You can only use recno field in select block!"
    end

    return nil if good_matches.empty?
    return get_matches_by_recno(query_type, filter, good_matches)
  end

  # Return record from table that matches supplied recno.
  def get_match_by_recno(query_type, filter, recno)
    result_struct = get_result_struct(query_type, filter)
    match_array = KBResultSet.new(self, filter, filter.collect { |f|
     @field_types[@field_names.index(f)] })

    tbl_rec = @table_class.new(self)

    rec = @db.engine.get_rec_by_recno(self, recno)
    return nil if rec.nil?
    tbl_rec.populate(rec)

    return create_result_rec(query_type, filter, result_struct,
     tbl_rec, rec)
  end

  # Return records from table that match select condition.
  def get_matches_by_recno(query_type, filter, recnos)
    result_struct = get_result_struct(query_type, filter)
    match_array = KBResultSet.new(self, filter, filter.collect { |f|
     @field_types[@field_names.index(f)] })

    tbl_rec = @table_class.new(self)

    @db.engine.get_recs_by_recno(self, recnos).each do |rec|
      next if rec.nil?
      tbl_rec.populate(rec)

      match_array << create_result_rec(query_type, filter,
       result_struct, tbl_rec, rec)
    end
    match_array
  end

end