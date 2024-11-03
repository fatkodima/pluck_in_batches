# frozen_string_literal: true

module PluckInBatches
  class Iterator # :nodoc:
    VALID_ORDERS = [:asc, :desc].freeze
    DEFAULT_ORDER = :asc

    def initialize(relation)
      @relation = relation
      @klass = relation.klass
    end

    def each(*columns, start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, cursor_column: @relation.primary_key, order: DEFAULT_ORDER, &block)
      if columns.empty?
        raise ArgumentError, "Call `pluck_each' with at least one column."
      end

      if block_given?
        each_batch(*columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, cursor_column: cursor_column, order: order) do |batch|
          batch.each(&block)
        end
      else
        enum_for(__callee__, *columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, cursor_column: cursor_column, order: order) do
          apply_limits(@relation, start, finish, build_batch_orders(order)).size
        end
      end
    end

    def each_batch(*columns, start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, cursor_column: @relation.primary_key, order: DEFAULT_ORDER)
      if columns.empty?
        raise ArgumentError, "Call `pluck_in_batches' with at least one column."
      end

      unless Array(order).all? { |ord| VALID_ORDERS.include?(ord) }
        raise ArgumentError, ":order must be :asc or :desc or an array consisting of :asc or :desc, got #{order.inspect}"
      end

      pluck_columns = columns.map do |column|
        if Arel.arel_node?(column)
          column
        else
          column.to_s
        end
      end

      cursor_columns = Array(cursor_column).map(&:to_s)
      cursor_column_indexes = cursor_column_indexes(pluck_columns, cursor_columns)
      missing_cursor_columns = cursor_column_indexes.count(&:nil?)
      cursor_column_indexes.each_with_index do |column_index, index|
        unless column_index
          cursor_column_indexes[index] = pluck_columns.size
          pluck_columns << cursor_columns[index]
        end
      end

      relation = @relation
      batch_orders = build_batch_orders(cursor_columns, order)

      unless block_given?
        return to_enum(__callee__, *columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, cursor_column: cursor_column, order: order) do
          total = apply_limits(relation, cursor_columns, start, finish, batch_orders).size
          (total - 1).div(batch_size) + 1
        end
      end

      if relation.arel.orders.present?
        act_on_ignored_order(error_on_ignore)
      end

      batch_limit = batch_size
      if relation.limit_value
        remaining   = relation.limit_value
        batch_limit = remaining if remaining < batch_limit
      end

      relation = relation.reorder(batch_orders.to_h).limit(batch_limit)
      relation = apply_limits(relation, cursor_columns, start, finish, batch_orders)
      relation.skip_query_cache! # Retaining the results in the query cache would undermine the point of batching
      batch_relation = relation

      loop do
        batch = batch_relation.pluck(*pluck_columns)
        break if batch.empty?

        cursor_column_offsets =
          if pluck_columns.size == 1
            Array(batch.last)
          else
            cursor_column_indexes.map.with_index do |column_index, index|
              batch.last[column_index || (batch.last.size - cursor_column_indexes.size + index)]
            end
          end

        missing_cursor_columns.times { batch.each(&:pop) }
        batch.flatten!(1) if columns.size == 1

        yield batch

        break if batch.length < batch_limit

        if @relation.limit_value
          remaining -= batch.length

          if remaining == 0
            # Saves a useless iteration when the limit is a multiple of the
            # batch size.
            break
          elsif remaining < batch_limit
            relation = relation.limit(remaining)
          end
        end

        _last_column, last_order = batch_orders.last
        operators = batch_orders.map do |_column, order| # rubocop:disable Lint/ShadowingOuterLocalVariable
          order == :desc ? :lteq : :gteq
        end
        operators[-1] = (last_order == :desc ? :lt : :gt)

        batch_relation = batch_condition(relation, cursor_columns, cursor_column_offsets, operators)
      end
    end

    private
      def cursor_column_indexes(columns, cursor_column)
        cursor_column.map do |column|
          columns.index(column) ||
            columns.index("#{@klass.table_name}.#{column}") ||
            columns.index("#{@klass.quoted_table_name}.#{@klass.connection.quote_column_name(column)}")
        end
      end

      def act_on_ignored_order(error_on_ignore)
        raise_error =
          if error_on_ignore.nil?
            if ar_version >= 7.0
              ActiveRecord.error_on_ignored_order
            else
              @klass.error_on_ignored_order
            end
          else
            error_on_ignore
          end

        message = "Scoped order is ignored, it's forced to be batch order."

        if raise_error
          raise ArgumentError, message
        elsif (logger = ActiveRecord::Base.logger)
          logger.warn(message)
        end
      end

      def apply_limits(relation, columns, start, finish, batch_orders)
        relation = apply_start_limit(relation, columns, start, batch_orders) if start
        relation = apply_finish_limit(relation, columns, finish, batch_orders) if finish
        relation
      end

      def apply_start_limit(relation, columns, start, batch_orders)
        operators = batch_orders.map do |_column, order|
          order == :desc ? :lteq : :gteq
        end
        batch_condition(relation, columns, start, operators)
      end

      def apply_finish_limit(relation, columns, finish, batch_orders)
        operators = batch_orders.map do |_column, order|
          order == :desc ? :gteq : :lteq
        end
        batch_condition(relation, columns, finish, operators)
      end

      def batch_condition(relation, columns, values, operators)
        cursor_positions = Array(columns).zip(Array(values), operators)

        first_clause_column, first_clause_value, operator = cursor_positions.pop
        where_clause = build_attribute_predicate(first_clause_column, first_clause_value, operator)

        cursor_positions.reverse_each do |column_name, value, operator| # rubocop:disable Lint/ShadowingOuterLocalVariable
          where_clause = build_attribute_predicate(column_name, value, operator == :lteq ? :lt : :gt).or(
            build_attribute_predicate(column_name, value, :eq).and(where_clause)
          )
        end

        relation.where(where_clause)
      end

      def build_attribute_predicate(column, value, operator)
        @relation.bind_attribute(column, value) { |attr, bind| attr.public_send(operator, bind) }
      end

      def build_batch_orders(cursor_columns, order)
        cursor_columns.zip(Array(order)).map do |column, ord|
          [column, ord || DEFAULT_ORDER]
        end
      end

      def ar_version
        ActiveRecord.version.to_s.to_f
      end
  end
end
