# frozen_string_literal: true

ActiveRecord::Schema.define do
  create_table :users, force: true do |t|
    t.string :name
  end

  create_table :subscribers, id: false, primary_key: :nick, force: true do |t|
    t.string :nick
    t.string :name
  end
end
