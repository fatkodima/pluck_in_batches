# frozen_string_literal: true

require "test_helper"

class PluckInBatchesTest < TestCase
  def test_pluck_each_requires_columns
    error = assert_raises(ArgumentError) do
      User.pluck_each
    end
    assert_match("Call `pluck_each' with at least one column.", error.message)
  end

  def test_pluck_each_should_return_an_enumerator_if_no_block_is_present
    ids = User.order(:id).ids
    assert_queries(1) do
      User.pluck_each(:id, batch_size: 100_000).with_index do |id, index|
        assert_equal ids[index], id
        assert_kind_of Integer, index
      end
    end
  end

  def test_pluck_each_should_return_values
    ids_and_names = User.order(:id).pluck(:id, :name)
    index = 0
    assert_queries(User.count + 1) do
      User.pluck_each(:id, :name, batch_size: 1) do |values|
        assert_equal ids_and_names[index], values
        index += 1
      end
    end
  end

  def test_pluck_in_batches_requires_columns
    error = assert_raises(ArgumentError) do
      User.pluck_in_batches
    end
    assert_match(error.message, "Call `pluck_in_batches' with at least one column.")
  end

  def test_pluck_in_batches_should_return_batches
    ids = User.order(:id).ids
    assert_queries(User.count + 1) do
      User.pluck_in_batches(:id, batch_size: 1).with_index do |batch, index|
        assert_kind_of Array, batch
        assert_equal ids[index], batch.first
      end
    end
  end

  def pluck_in_batches_desc_order
    ids = User.order(id: :desc).ids
    assert_queries(User.count + 1) do
      User.pluck_in_batches(:id, batch_size: 1, order: :desc).with_index do |batch, index|
        assert_kind_of Array, batch
        assert_equal ids[index], batch.first
      end
    end
  end

  def test_pluck_in_batches_should_start_from_the_start_option
    assert_queries(User.count) do
      User.pluck_in_batches(:id, batch_size: 1, start: 2) do |batch|
        assert_kind_of Array, batch
        assert_kind_of Integer, batch.first
      end
    end
  end

  def test_pluck_in_batches_should_end_at_the_finish_option
    assert_queries(6) do
      User.pluck_in_batches(:id, batch_size: 1, finish: 5) do |batch|
        assert_kind_of Array, batch
        assert_kind_of Integer, batch.first
      end
    end
  end

  def test_pluck_in_batches_multiple_columns
    ids_and_names = User.order(:id).pluck(:id, :name)
    assert_queries(User.count + 1) do
      User.pluck_in_batches(:id, :name, batch_size: 1).with_index do |batch, index|
        assert_kind_of Array, batch
        assert_kind_of Array, batch.first
        assert_equal ids_and_names[index], batch.first
      end
    end
  end

  def test_pluck_in_batches_id_is_missing
    names = User.order(:id).pluck(:name)
    assert_queries(User.count + 1) do
      User.pluck_in_batches(:name, batch_size: 1).with_index do |batch, index|
        assert_kind_of Array, batch
        assert_equal names[index], batch.first
      end
    end
  end

  def test_pluck_in_batches_fully_qualified_id_is_present
    ids_and_names = User.order(:id).pluck(:id, :name)
    assert_queries(User.count + 1) do
      User.pluck_in_batches("users.id", :name, batch_size: 1).with_index do |batch, index|
        assert_kind_of Array, batch
        assert_kind_of Array, batch.first
        assert_equal ids_and_names[index], batch.first
      end
    end
  end

  def test_pluck_in_batches_shouldnt_execute_query_unless_needed
    count = User.count
    assert_queries(2) do
      User.pluck_in_batches(:id, batch_size: count) { |batch| assert_kind_of Array, batch }
    end

    assert_queries(1) do
      User.pluck_in_batches(:id, batch_size: count + 1) { |batch| assert_kind_of Array, batch }
    end
  end

  def test_pluck_in_batches_should_ignore_the_order_default_scope
    # First user with name scope
    first_user = UserWithDefaultScope.first
    ids = []
    UserWithDefaultScope.pluck_in_batches(:id) do |batch|
      ids.concat(batch)
    end
    # ids.first will be ordered using id only. Name order scope should not apply here
    assert_not_equal first_user.id, ids.first
    assert_equal User.first.id, ids.first
  end

  def test_pluck_in_batches_should_error_on_ignore_the_order
    error = assert_raises(ArgumentError) do
      UserWithDefaultScope.pluck_in_batches(:id, error_on_ignore: true) {}
    end
    assert_match("Scoped order is ignored, it's forced to be batch order.", error.message)
  end

  def test_pluck_in_batches_should_not_error_if_config_overridden
    with_error_on_ignored_order(UserWithDefaultScope, true) do
      assert_nothing_raised do
        UserWithDefaultScope.pluck_in_batches(:id, error_on_ignore: false) {}
      end
    end
  end

  def test_pluck_in_batches_should_error_on_config_specified_to_error
    with_error_on_ignored_order(UserWithDefaultScope, true) do
      error = assert_raises(ArgumentError) do
        UserWithDefaultScope.pluck_in_batches(:id) {}
      end
      assert_match("Scoped order is ignored, it's forced to be batch order.", error.message)
    end
  end

  def test_pluck_in_batches_should_not_error_by_default
    assert_nothing_raised do
      UserWithDefaultScope.pluck_in_batches(:id) {}
    end
  end

  def test_pluck_in_batches_should_not_ignore_the_default_scope_if_it_is_other_than_order
    default_scope = SpecialUserWithDefaultScope.all
    ids = []
    SpecialUserWithDefaultScope.pluck_in_batches(:id) do |batch|
      ids.concat(batch)
    end
    assert_equal default_scope.pluck(:id).sort, ids.sort
  end

  def test_pluck_in_batches_should_use_any_column_as_primary_key
    nick_order_subscribers = Subscriber.order(nick: :asc)
    start_nick = nick_order_subscribers.second.nick

    names = []
    Subscriber.pluck_in_batches(:name, batch_size: 1, start: start_nick) do |batch|
      names.concat(batch)
    end

    assert_equal nick_order_subscribers[1..].map(&:name), names
  end

  def test_pluck_in_batches_should_return_an_enumerator
    enum = nil
    assert_no_queries do
      enum = User.pluck_in_batches(:id, batch_size: 1)
    end
    assert_queries(4) do
      enum.first(4) do |batch|
        assert_kind_of Array, batch
        assert_kind_of Integer, batch.first
      end
    end
  end

  def test_pluck_in_batches_should_honor_limit_if_passed_a_block
    limit = User.count - 1
    total = 0

    User.limit(limit).pluck_in_batches(:id) do |batch|
      total += batch.size
    end

    assert_equal limit, total
  end

  def test_pluck_in_batches_should_honor_limit_if_no_block_is_passed
    limit = User.count - 1
    total = 0

    User.limit(limit).pluck_in_batches(:id).each do |batch|
      total += batch.size
    end

    assert_equal limit, total
  end

  def test_pluck_in_batches_should_return_a_sized_enumerator
    assert_equal 20, User.pluck_in_batches(:id, batch_size: 1).size
    assert_equal 10, User.pluck_in_batches(:id, batch_size: 2).size
    assert_equal 9, User.pluck_in_batches(:id, batch_size: 2, start: 4).size
    assert_equal 7, User.pluck_in_batches(:id, batch_size: 3).size
    assert_equal 1, User.pluck_in_batches(:id, batch_size: 10_000).size
  end

  module CompositePrimaryKeys
    def test_pluck_in_batches_should_iterate_over_composite_primary_key
      skip if ar_version < 7.1

      ids = Product.order(:shop_id, :id).ids
      Product.pluck_in_batches(:shop_id, :id, batch_size: 1).with_index do |batch, index|
        assert_kind_of Array, batch
        assert_equal ids[index], batch.first
      end
    end

    def test_pluck_in_batches_over_composite_primary_key_when_one_column_is_missing
      skip if ar_version < 7.1

      ids_and_names = Product.order(:shop_id, :id).pluck(:id, :name)
      Product.pluck_in_batches(:id, :name, batch_size: 1).with_index do |batch, index|
        assert_kind_of Array, batch
        assert_equal ids_and_names[index], batch.first
      end
    end

    def test_pluck_in_batches_over_composite_primary_key_should_start_from_the_start_option
      skip if ar_version < 7.1

      product = Product.second
      batch = Product.pluck_in_batches(:name, batch_size: 1, start: product.id).first
      assert_equal product.name, batch.first
    end

    def test_pluck_in_batches_over_composite_primary_key_should_end_at_the_finish_option
      skip if ar_version < 7.1

      product = Product.second_to_last
      batch = Product.pluck_in_batches(:name, batch_size: 1, finish: product.id).reverse_each.first
      assert_equal product.name, batch.first
    end
  end
  include CompositePrimaryKeys

  def test_pluck_each_should_iterate_over_custom_cursor_column
    ids = Package.order(:version).ids
    Package.pluck_each(:id, batch_size: 1, cursor_column: :version).with_index do |id, index|
      assert_equal ids[index], id
    end
  end

  def test_pluck_in_batches_should_iterate_over_custom_cursor_column
    ids = Package.order(:version).ids
    Package.pluck_in_batches(:id, batch_size: 1, cursor_column: :version).with_index do |batch, index|
      assert_equal ids[index], batch.first
    end
  end

  private
    def with_error_on_ignored_order(klass, value)
      if ar_version >= 7.0
        prev = ActiveRecord.error_on_ignored_order
        ActiveRecord.error_on_ignored_order = value
      else
        prev = klass.error_on_ignored_order
        klass.error_on_ignored_order = value
      end
      yield
    ensure
      if ar_version >= 7.0
        ActiveRecord.error_on_ignored_order = prev
      else
        klass.error_on_ignored_order = prev
      end
    end

    def ar_version
      ActiveRecord.version.to_s.to_f
    end
end
