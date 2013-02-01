module KirbyBase

  class Base
    include DRb::DRbUndumped
    include KBTypeConversionsMixin

    VERSION = "3.0.0.dev"

    attr_reader :engine

    attr_accessor(:connect_type, :host, :port, :path, :ext, :memo_blob_path,
     :delay_index_creation)

    # Create a new database instance.
    #
    # *connect_type*:: Symbol (:local, :client, :server) specifying role to
    #          play.
    # *host*:: String containing IP address or DNS name of server hosting
    #      database. (Only valid if connect_type is :client.)
    # *port*:: Integer specifying port database server is listening on.
    #      (Only valid if connect_type is :client.)
    # *path*:: String specifying path to location of database tables.
    # *ext*:: String specifying extension of table files.
    # *memo_blob_path*:: String specifying path to location of memo/blob
    #          files.
    # *delay_index_creation*:: Boolean specifying whether to delay index
    #              creation for each table until that table is
    #              requested by user.
    def initialize(connect_type=:local, host=nil, port=nil, path='./', ext='.tbl', memo_blob_path='./', delay_index_creation=false)
      @connect_type = connect_type
      @host = host
      @port = port
      @path = path
      @ext = ext
      @memo_blob_path = memo_blob_path
      @delay_index_creation = delay_index_creation

      # See if user specified any method arguments via a code block.
      yield self if block_given?

      # After the yield, make sure the user doesn't change any of these
      # instance variables.
      class << self
        private(:connect_type=, :host=, :path=, :ext=, :memo_blob_path=,
         :delay_index_creation=)
      end

      # Did user supply full and correct arguments to method?
      raise ArgumentError, 'Invalid connection type specified' unless (
       [:local, :client, :server].include?(@connect_type))
      raise "Must specify hostname or IP address!" if \
       @connect_type == :client and @host.nil?
      raise "Must specify port number!" if @connect_type == :client and \
       @port.nil?
      raise "Invalid path!" if @path.nil?
      raise "Invalid extension!" if @ext.nil?
      raise "Invalid memo/blob path!" if @memo_blob_path.nil?

      @table_hash = {}

      # If running as a client, start druby and connect to server.
      if client?
        DRb.start_service()
        @server = DRbObject.new(nil, 'druby://%s:%d' % [@host, @port])
        @engine = @server.engine
        @path = @server.path
        @ext = @server.ext
        @memo_blob_path = @server.memo_blob_path
      else
        @engine = KBEngine.create_called_from_database_instance(self)
      end

      # The reason why I create all the table instances here is two
      # reasons:  (1) I want all of the tables ready to go when a user
      # does a #get_table, so they don't have to wait for the instance
      # to be created, and (2) I want all of the table indexes to get
      # created at the beginning during database initialization so that
      # they are ready for the user to use.  Since index creation
      # happens when the table instance is first created, I go ahead and
      # create table instances right off the bat.
      #
      # You can delay index creation until the first time the index is
      # used.
      if @delay_index_creation
      else
        @engine.tables.each do |tbl|
          @table_hash[tbl] = \
           KBTable.create_called_from_database_instance(self, tbl,
           File.join(@path, tbl.to_s + @ext))
        end
      end
    end

    # Is this running as a server?
    def server?
      @connect_type == :server
    end

    # Is this running as a client?
    def client?
      @connect_type == :client
    end

    # Is this running in single-user, embedded mode?
    def local?
      @connect_type == :local
    end

    # Return an array containing the names of all tables in this database.
    def tables
      @engine.tables
    end

    # Return a reference to the requested table.
    # *name*:: Symbol of table name.
    def get_table(name)
      raise('Do not call this method from a server instance!') if server?
      raise(ArgumentError, 'Table name must be a symbol!') unless name.is_a?(Symbol)
      raise('Table not found!') unless table_exists?(name)

      if @table_hash.has_key?(name)
        @table_hash[name]
      else
        @table_hash[name] = \
         KBTable.create_called_from_database_instance(self, name,
          File.join(@path, name.to_s + @ext))
        @table_hash[name]
      end
    end

    # Create new table and return a reference to the new table.
    # *name*:: Symbol of table name.
    # *field_defs*:: List of field names (Symbols), field types (Symbols),
    #        field indexes, and field extras (Indexes, Lookups,
    #        Link_manys, Calculateds, etc.)
    # *Block*:: Optional code block allowing you to set the following:
    # *encrypt*:: true/false specifying whether table should be encrypted.
    # *record_class*:: Class or String specifying the user create class that
    #          will be associated with table records.
    def create_table(name=nil, *field_defs)
      raise "Can't call #create_table from server!" if server?

      t_struct = Struct.new(:name, :field_defs, :encrypt, :record_class)
      t = t_struct.new
      t.name = name
      t.field_defs = field_defs
      t.encrypt = false
      t.record_class = 'Struct'

      yield t if block_given?

      raise "Name must be a symbol!" unless t.name.is_a?(Symbol)
      raise "No table name specified!" if t.name.nil?
      raise "No table field definitions specified!" if t.field_defs.nil?

      # Can't create a table that already exists!
      raise "Table already exists!" if table_exists?(t.name)

      raise 'Must have a field type for each field name' \
       unless t.field_defs.size.remainder(2) == 0

      # Check to make sure there are no duplicate field names.
      temp_field_names = []
      (0...t.field_defs.size).step(2) do |x|
        temp_field_names << t.field_defs[x]
      end
      raise 'Duplicate field names are not allowed!' unless temp_field_names == temp_field_names.uniq

      temp_field_defs = []
      (0...t.field_defs.size).step(2) do |x|
        temp_field_defs << build_header_field_string(t.field_defs[x],
         t.field_defs[x+1])
      end

      @engine.new_table(t.name, temp_field_defs, t.encrypt,
       t.record_class.to_s)

      get_table(t.name)
    end

    def build_header_field_string(field_name_def, field_type_def)
      # Put field name at start of string definition.
      temp_field_def = field_name_def.to_s + ':'

      # If field type is a hash, that means that it is not just a
      # simple field.  Either is is a key field, it is being used in an
      # index, it is a default value, it is a required field, it is a
      # Lookup field, it is a Link_many field, or it is a Calculated
      # field.  This next bit of code is to piece together a proper
      # string so that it can be written out to the header rec.
      if field_type_def.is_a?(Hash)
        raise 'Missing :DataType key in field_type hash!' unless field_type_def.has_key?(:DataType)

        temp_type = field_type_def[:DataType]

        raise 'Invalid field type: %s' % temp_type unless KBTable.valid_field_type?(temp_type)

        temp_field_def += field_type_def[:DataType].to_s

        # Check if this field is a key for the table.
        if field_type_def.has_key?(:Key)
          temp_field_def += ':Key->true'
        end

        # Check for Index definition.
        if field_type_def.has_key?(:Index)
          raise 'Invalid field type for index: %s' % temp_type unless KBTable.valid_index_type?(temp_type)

          temp_field_def += ':Index->' + field_type_def[:Index].to_s
        end

        # Check for Default value definition.
        if field_type_def.has_key?(:Default)
          raise 'Cannot set default value for this type: ' + \
           '%s' % temp_type unless KBTable.valid_default_type?(
           temp_type)

          unless field_type_def[:Default].nil?
            raise 'Invalid default value ' + \
             '%s for column %s' % [field_type_def[:Default],
             field_name_def] unless KBTable.valid_data_type?(
             temp_type, field_type_def[:Default])

            temp_field_def += ':Default->' + \
             convert_to_encoded_string(temp_type,
             field_type_def[:Default])
          end
        end

        # Check for Required definition.
        if field_type_def.has_key?(:Required)
          raise 'Required must be true or false!' unless [true, false].include?(field_type_def[:Required])

          temp_field_def += \
           ':Required->%s' % field_type_def[:Required]
        end

        # Check for Lookup field, Link_many field, Calculated field
        # definition.
        if field_type_def.has_key?(:Lookup)
          if field_type_def[:Lookup].is_a?(Array)
            temp_field_def += \
             ':Lookup->%s.%s' % field_type_def[:Lookup]
          else
            tbl = get_table(field_type_def[:Lookup])
            temp_field_def += \
             ':Lookup->%s.%s' % [field_type_def[:Lookup],
             tbl.lookup_key]
          end
        elsif field_type_def.has_key?(:Link_many)
          raise 'Field type for Link_many field must be :ResultSet' unless temp_type == :ResultSet
          temp_field_def += \
           ':Link_many->%s=%s.%s' % field_type_def[:Link_many]
        elsif field_type_def.has_key?(:Calculated)
          temp_field_def += \
           ':Calculated->%s' % field_type_def[:Calculated]
        end
      else
        if KBTable.valid_field_type?(field_type_def)
          temp_field_def += field_type_def.to_s
        elsif table_exists?(field_type_def)
          tbl = get_table(field_type_def)
          temp_field_def += \
           '%s:Lookup->%s.%s' % [tbl.field_types[
           tbl.field_names.index(tbl.lookup_key)], field_type_def,
           tbl.lookup_key]
        else
          raise 'Invalid field type: %s' % field_type_def
        end
      end

      temp_field_def
    end

    # Rename a table.
    #
    # *old_tablename*:: Symbol of old table name.
    # *new_tablename*:: Symbol of new table name.
    def rename_table(old_tablename, new_tablename)
      raise "Cannot rename table running in client mode!" if client?
      raise "Table does not exist!" unless table_exists?(old_tablename)
      raise(ArgumentError, 'Existing table name must be a symbol!') unless old_tablename.is_a?(Symbol)
      raise(ArgumentError, 'New table name must be a symbol!') unless new_tablename.is_a?(Symbol)
      raise "Table already exists!" if table_exists?(new_tablename)

      @table_hash.delete(old_tablename)
      @engine.rename_table(old_tablename, new_tablename)
      get_table(new_tablename)
    end

    # Delete a table.
    #
    # *tablename*:: Symbol of table name.
    def drop_table(tablename)
      raise(ArgumentError, 'Table name must be a symbol!') unless tablename.is_a?(Symbol)
      raise "Table does not exist!" unless table_exists?(tablename)

      @table_hash.delete(tablename)
      @engine.delete_table(tablename)
    end

    # Return true if table exists.
    #
    # *tablename*:: Symbol of table name.
    def table_exists?(tablename)
      raise(ArgumentError, 'Table name must be a symbol!') unless tablename.is_a?(Symbol)

      @engine.table_exists?(tablename)
    end

  end

end