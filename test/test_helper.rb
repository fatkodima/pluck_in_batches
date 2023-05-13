# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "pluck_in_batches"

require "sqlite3"
require "minitest/autorun"

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")

if ENV["VERBOSE"]
  ActiveRecord::Base.logger = ActiveSupport::Logger.new($stdout)
else
  ActiveRecord::Base.logger = ActiveSupport::Logger.new("debug.log", 1, 100 * 1024 * 1024) # 100 mb
  ActiveRecord::Migration.verbose = false
end

require_relative "support/schema"
require_relative "support/models"

def prepare_database
  # Create users
  values = 20.times.map do |i|
    id = i + 1
    "(#{id}, 'User-#{id}')"
  end.join(", ")
  ActiveRecord::Base.connection.execute("INSERT INTO users (id, name) VALUES #{values}")

  # Create subscribers
  values = 10.times.map do |i|
    id = i + 1
    "('nick-#{id}', 'User-#{id}')"
  end.join(", ")
  ActiveRecord::Base.connection.execute("INSERT INTO subscribers (nick, name) VALUES #{values}")

  # Create products
  values = 20.times.map do |i|
    id = i + 1
    shop_id = rand(1..5)
    "(#{shop_id}, #{id}, '(#{shop_id}, #{id})')"
  end.join(", ")
  ActiveRecord::Base.connection.execute("INSERT INTO products (shop_id, id, name) VALUES #{values}")
end

prepare_database

class TestCase < Minitest::Test
  alias assert_not_equal refute_equal

  def assert_nothing_raised
    yield.tap { assert(true) } # rubocop:disable Minitest/UselessAssertion
  rescue StandardError => e
    raise Minitest::UnexpectedError, e
  end

  def assert_queries(num, &block)
    ActiveRecord::Base.connection.materialize_transactions
    count = 0

    ActiveSupport::Notifications.subscribe("sql.active_record") do |_name, _start, _finish, _id, payload|
      count += 1 unless ["SCHEMA", "TRANSACTION"].include? payload[:name]
    end

    result = block.call
    assert_equal num, count, "#{count} instead of #{num} queries were executed."
    result
  end

  def assert_no_queries(&block)
    assert_queries(0, &block)
  end
end
