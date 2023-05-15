# PluckInBatches

ActiveRecord comes with `find_each` and `find_in_batches` methods to batch process records from a database.
ActiveRecord also has the `pluck` method which allows the selection of a set of fields without pulling
the entire record into memory.

This gem combines these ideas and provides `pluck_each` and `pluck_in_batches` methods to allow
batch processing of plucked fields from the database.

It performs half of the number of SQL queries, allocates up to half of the memory and is up to 2x faster
(or more, depending on how far is your database from the application) than the available alternative:

```ruby
# Before
User.in_batches do |batch|
  emails = batch.pluck(:emails)
  # do something with emails
end

# Now, using this gem (up to 2x faster)
User.pluck_in_batches(:email) do |emails|
  # do something with emails
end
```

## Requirements

- Ruby 2.7+
- ActiveRecord 6+

If you need support for older versions, [open an issue](https://github.com/fatkodima/pluck_in_batches/issues/new).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'pluck_in_batches'
```

And then execute:

```sh
$ bundle
```

Or install it yourself as:

```sh
$ gem install pluck_in_batches
```

## Usage

### `pluck_each`

Behaves similarly to `find_each` ActiveRecord's method, but yields each set of values corresponding
to the specified columns.

```ruby
# Single column
User.where(active: true).pluck_each(:email) do |email|
  # do something with email
end

# Multiple columns
User.where(active: true).pluck_each(:id, :email) do |id, email|
  # do something with id and email
end
```

### `pluck_in_batches`

Behaves similarly to `in_batches` ActiveRecord's method, but yields each batch
of values corresponding to the specified columns.

```ruby
# Single column
User.where("age > 21").pluck_in_batches(:email) do |emails|
  jobs = emails.map { |email| PartyReminderJob.new(email) }
  ActiveJob.perform_all_later(jobs)
end

# Multiple columns
User.pluck_in_batches(:name, :email).with_index do |group, index|
  puts "Processing group ##{index}"
  jobs = group.map { |name, email| PartyReminderJob.new(name, email) }
  ActiveJob.perform_all_later(jobs)
end
```

Both methods support the following configuration options:

* `:batch_size` - Specifies the size of the batch. Defaults to 1000.
* `:start` - Specifies the primary key value to start from, inclusive of the value.
* `:finish` - Specifies the primary key value to end at, inclusive of the value.
* `:error_on_ignore` - Overrides the application config to specify if an error should be raised when
  an order is present in the relation.
* :cursor_column - Specifies the column(s) on which the iteration should be done.
  This column(s) should be orderable (e.g. an integer or string). Defaults to primary key.
* `:order` - Specifies the primary key order (can be `:asc` or `:desc`). Defaults to `:asc`.

## Development

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/fatkodima/pluck_in_batches.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
