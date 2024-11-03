## master (unreleased)

## 0.3.0 (2024-11-03)

- Support plucking custom Arel columns

  ```ruby
  User.pluck_in_batches(:id, Arel.sql("json_extract(users.metadata, '$.rank')"))
  ```

## 0.2.0 (2023-07-24)

- Support specifying per cursor column ordering when batching

  ```ruby
  Book.pluck_in_batches(:title, cursor_columns: [:author_id, :version], order: [:asc, :desc])
  ```

- Add `:of` as an alias for `:batch_size` option

## 0.1.0 (2023-05-16)

- First release
