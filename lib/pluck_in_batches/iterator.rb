# frozen_string_literal: true

module PluckInBatches
  class Iterator # :nodoc:
    def initialize(relation)
      @relation = relation
      @klass = relation.klass
    end

    def each(*columns, start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, order: :asc, &block)
      if columns.empty?
        raise ArgumentError, "Call `pluck_each' with at least one column."
      end

      if block_given?
        each_batch(*columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order) do |batch|
          batch.each(&block)
        end
      else
        enum_for(__callee__, *columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order) do
          apply_limits(@relation, start, finish, order).size
        end
      end
    end

    def each_batch(*columns, start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, order: :asc)
      if columns.empty?
        raise ArgumentError, "Call `pluck_in_batches' with at least one column."
      end

      unless order == :asc || order == :desc
        raise ArgumentError, ":order must be :asc or :desc, got #{order.inspect}"
      end

      pluck_columns = columns.map(&:to_s)
      primary_key_indexes = primary_key_indexes(pluck_columns)
      missing_primary_key_columns = primary_key_indexes.count(&:nil?)
      primary_key_indexes.each_with_index do |column_index, index|
        unless column_index
          primary_key_indexes[index] = pluck_columns.size
          pluck_columns << primary_key[index]
        end
      end

      relation = @relation

      unless block_given?
        return to_enum(__callee__, *columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order) do
          total = apply_limits(relation, start, finish, order).size
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

      relation = relation.reorder(*batch_order(order)).limit(batch_limit)
      relation = apply_limits(relation, start, finish, order)
      relation.skip_query_cache! # Retaining the results in the query cache would undermine the point of batching
      batch_relation = relation

      loop do
        batch = batch_relation.pluck(*pluck_columns)
        break if batch.empty?

        primary_key_offsets =
          if pluck_columns.size == 1
            Array(batch.last)
          else
            primary_key_indexes.map.with_index do |column_index, index|
              batch.last[column_index || (batch.last.size - primary_key_indexes.size + index)]
            end
          end

        missing_primary_key_columns.times { batch.each(&:pop) }
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

        batch_relation = batch_condition(
          relation, primary_key, primary_key_offsets, order == :desc ? :lt : :gt
        )
      end
    end

    private
      def primary_key_indexes(columns)
        primary_key.map do |column|
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

      def apply_limits(relation, start, finish, order)
        relation = apply_start_limit(relation, start, order) if start
        relation = apply_finish_limit(relation, finish, order) if finish
        relation
      end

      def apply_start_limit(relation, start, order)
        batch_condition(relation, primary_key, start, order == :desc ? :lteq : :gteq)
      end

      def apply_finish_limit(relation, finish, order)
        batch_condition(relation, primary_key, finish, order == :desc ? :gteq : :lteq)
      end

      def batch_condition(relation, columns, values, operator)
        columns = Array(columns)
        values = Array(values)
        cursor_positions = columns.zip(values)

        first_clause_column, first_clause_value = cursor_positions.pop
        where_clause = build_attribute_predicate(first_clause_column, first_clause_value, operator)

        cursor_positions.reverse_each do |column_name, value|
          where_clause = build_attribute_predicate(column_name, value, operator == :lteq ? :lt : :gt).or(
            build_attribute_predicate(column_name, value, :eq).and(where_clause)
          )
        end

        relation.where(where_clause)
      end

      def build_attribute_predicate(column, value, operator)
        @relation.bind_attribute(column, value) { |attr, bind| attr.public_send(operator, bind) }
      end

      def batch_order(order)
        primary_key.map do |column|
          @relation.arel_table[column].public_send(order)
        end
      end

      def primary_key
        Array(@relation.primary_key)
      end

      def ar_version
        ActiveRecord.version.to_s.to_f
      end
  end
end
