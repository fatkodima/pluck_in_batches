# frozen_string_literal: true

module PluckInBatches
  class Iterator # :nodoc:
    def initialize(relation)
      @relation = relation
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
      primary_key_index = primary_key_index(pluck_columns)
      pluck_columns << primary_key unless primary_key_index

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
      if @relation.limit_value
        remaining   = relation.limit_value
        batch_limit = remaining if remaining < batch_limit
      end

      relation = relation.reorder(batch_order(order)).limit(batch_limit)
      relation = apply_limits(relation, start, finish, order)
      relation.skip_query_cache! # Retaining the results in the query cache would undermine the point of batching
      batch_relation = relation

      loop do
        batch = batch_relation.pluck(*pluck_columns)
        break if batch.empty?

        primary_key_offset =
          if pluck_columns.size == 1
            batch.last
          else
            batch.last[primary_key_index || -1]
          end

        batch.each(&:pop) unless primary_key_index
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

        batch_relation = relation.where(
          relation.bind_attribute(primary_key, primary_key_offset) { |attr, bind| attr.public_send(order == :desc ? :lt : :gt, bind) }
        )
      end
    end

    private
      def primary_key_index(columns)
        columns.index(primary_key) ||
          columns.index("#{@relation.table_name}.#{primary_key}") ||
          columns.index("#{@relation.quoted_table_name}.#{@relation.quoted_primary_key}")
      end

      def act_on_ignored_order(error_on_ignore)
        raise_error =
          if error_on_ignore.nil?
            if ar_version >= 7.0
              ActiveRecord.error_on_ignored_order
            else
              @relation.klass.error_on_ignored_order
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
        relation.where(
          relation.bind_attribute(primary_key, start) { |attr, bind| attr.public_send(order == :desc ? :lteq : :gteq, bind) }
        )
      end

      def apply_finish_limit(relation, finish, order)
        relation.where(
          relation.bind_attribute(primary_key, finish) { |attr, bind| attr.public_send(order == :desc ? :gteq : :lteq, bind) }
        )
      end

      def batch_order(order)
        @relation.arel_table[primary_key].public_send(order)
      end

      def primary_key
        @relation.primary_key
      end

      def ar_version
        ActiveRecord.version.to_s.to_f
      end
  end
end
