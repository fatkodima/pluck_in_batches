# frozen_string_literal: true

module PluckInBatches
  module Extensions
    module ModelExtension
      delegate :pluck_each, :pluck_in_batches, to: :all
    end

    module RelationExtension
      # Yields each set of values corresponding to the specified columns that was found
      # by the passed options. If one column specified - returns its value, if an array of columns -
      # returns an array of values.
      #
      # See #pluck_in_batches for all the details.
      #
      def pluck_each(*columns, start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, order: :asc, &block)
        iterator = Iterator.new(self)
        iterator.each(*columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order, &block)
      end

      # Yields each batch of values corresponding to the specified columns that was found
      # by the passed options as an array.
      #
      #   User.where("age > 21").pluck_in_batches(:email) do |emails|
      #     jobs = emails.map { |email| PartyReminderJob.new(email) }
      #     ActiveJob.perform_all_later(jobs)
      #   end
      #
      # If you do not provide a block to #pluck_in_batches, it will return an Enumerator
      # for chaining with other methods:
      #
      #   User.pluck_in_batches(:name, :email).with_index do |group, index|
      #     puts "Processing group ##{index}"
      #     jobs = group.map { |name, email| PartyReminderJob.new(name, email) }
      #     ActiveJob.perform_all_later(jobs)
      #   end
      #
      # ==== Options
      # * <tt>:batch_size</tt> - Specifies the size of the batch. Defaults to 1000.
      # * <tt>:start</tt> - Specifies the primary key value to start from, inclusive of the value.
      # * <tt>:finish</tt> - Specifies the primary key value to end at, inclusive of the value.
      # * <tt>:error_on_ignore</tt> - Overrides the application config to specify if an error should be raised when
      #   an order is present in the relation.
      # * <tt>:order</tt> - Specifies the primary key order (can be +:asc+ or +:desc+). Defaults to +:asc+.
      #
      # Limits are honored, and if present there is no requirement for the batch
      # size: it can be less than, equal to, or greater than the limit.
      #
      # The options +start+ and +finish+ are especially useful if you want
      # multiple workers dealing with the same processing queue. You can make
      # worker 1 handle all the records between id 1 and 9999 and worker 2
      # handle from 10000 and beyond by setting the +:start+ and +:finish+
      # option on each worker.
      #
      #   # Let's process from record 10_000 on.
      #   User.pluck_in_batches(:email, start: 10_000) do |emails|
      #     jobs = emails.map { |email| PartyReminderJob.new(email) }
      #     ActiveJob.perform_all_later(jobs)
      #   end
      #
      # NOTE: Order can be ascending (:asc) or descending (:desc). It is automatically set to
      # ascending on the primary key ("id ASC").
      # This also means that this method only works when the primary key is
      # orderable (e.g. an integer or string).
      #
      # NOTE: By its nature, batch processing is subject to race conditions if
      # other processes are modifying the database.
      #
      def pluck_in_batches(*columns, start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, order: :asc, &block)
        iterator = Iterator.new(self)
        iterator.each_batch(*columns, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order, &block)
      end
    end
  end
end
