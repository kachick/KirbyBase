class KBResultSet
  #-----------------------------------------------------------------------
  # KBResultSet.reverse
  #-----------------------------------------------------------------------
  def KBResultSet.reverse(sort_field)
    return [sort_field, :desc]
  end

  #-----------------------------------------------------------------------
  # initialize
  #-----------------------------------------------------------------------
  def initialize(table, filter, filter_types, *values)
    @table = table
    @filter = filter
    @filter_types = filter_types
    @values = values

    @filter.each do |f|
      get_meth_str = <<-END_OF_STRING
      def #{f}()
        if defined?(@#{f}) then
          return @#{f}
        else
          @#{f} = self.collect { |x| x.#{f} }
          return @#{f}
        end
      end
      END_OF_STRING
      self.class.class_eval(get_meth_str)
    end
  end

  #-----------------------------------------------------------------------
  # to_ary
  #-----------------------------------------------------------------------
  def to_ary
    @values.dup
  end

  def <<(value)
    @values << value
    self
  end

  include Enumerable

  def each(&block)
    @values.each(&block)
  end

  def size
    @values.size
  end
  alias length size

  def [](key)
    @values[key]
  end

  #-----------------------------------------------------------------------
  # set
  #-----------------------------------------------------------------------
  #++
  # Update record(s) in table, return number of records updated.
  #
  def set(*updates, &update_cond)
    raise 'Cannot specify both a hash and a proc for method #set!' \
     unless updates.empty? or update_cond.nil?

    raise 'Must specify update proc or hash for method #set!' if \
     updates.empty? and update_cond.nil?

    if updates.empty?
      @table.set(self, update_cond)
    else
      @table.set(self, updates)
    end
  end

  #-----------------------------------------------------------------------
  # sort
  #-----------------------------------------------------------------------
  def sort(*sort_fields)
    sort_fields_arrs = []
    sort_fields.each do |f|
      if f.to_s[0..0] == '-'
        sort_fields_arrs << [f.to_s[1..-1].to_sym, :desc]
      elsif f.to_s[0..0] == '+'
        sort_fields_arrs << [f.to_s[1..-1].to_sym, :asc]
      else
        sort_fields_arrs << [f, :asc]
      end
    end

    sort_fields_arrs.each do |f|
      raise "Invalid sort field" unless @filter.include?(f[0])
    end

    sorted = @values.sort{|a,b|
      x = []
      y = []
      sort_fields_arrs.each do |s|
        if [:Integer, :Float].include?(
         @filter_types[@filter.index(s[0])])
          a_value = a.send(s[0]) || 0
          b_value = b.send(s[0]) || 0
        else
          a_value = a.send(s[0])
          b_value = b.send(s[0])
        end
        if s[1] == :desc
          x << b_value
          y << a_value
        else
          x << a_value
          y << b_value
        end
      end

      x <=> y
    }

    return self.class.new(@table, @filter, @filter_types, *sorted)
  end

  #-----------------------------------------------------------------------
  # to_report
  #-----------------------------------------------------------------------
  def to_report(recs_per_page=0, print_rec_sep=false)
    result = collect { |r| @filter.collect {|f| r.send(f)} }

    # How many records before a formfeed.
    delim = ' | '

    # columns of physical rows
    columns = [@filter].concat(result).transpose

    max_widths = columns.collect { |c|
      c.max { |a,b| a.to_s.size <=> b.to_s.size }.to_s.size
    }

    row_dashes = '-' * (max_widths.inject {|sum, n| sum + n} +
     delim.size * (max_widths.size - 1))

    justify_hash = { :String => :ljust, :Integer => :rjust,
     :Float => :rjust, :Boolean => :ljust, :Date => :ljust,
     :Time => :ljust, :DateTime => :ljust }

    header_line = @filter.zip(max_widths, @filter.collect { |f|
      @filter_types[@filter.index(f)] }).collect { |x,y,z|
         x.to_s.send(justify_hash[z], y) }.join(delim)

    output = ''
    recs_on_page_cnt = 0

    result.each do |row|
      if recs_on_page_cnt == 0
        output << header_line + "\n" << row_dashes + "\n"
      end

      output << row.zip(max_widths, @filter.collect { |f|
        @filter_types[@filter.index(f)] }).collect { |x,y,z|
          x.to_s.send(justify_hash[z], y) }.join(delim) + "\n"

      output << row_dashes + '\n' if print_rec_sep
      recs_on_page_cnt += 1

      if recs_per_page > 0 and (recs_on_page_cnt ==
       num_recs_per_page)
        output << '\f'
        recs_on_page_count = 0
      end
    end
    return output
  end
end