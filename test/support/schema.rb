# frozen_string_literal: true

ActiveRecord::Schema.define do
  create_table :users, force: true do |t|
    t.string :name
  end

  create_table :subscribers, id: false, primary_key: :nick, force: true do |t|
    t.string :nick
    t.string :name
  end

  create_table :products, primary_key: [:shop_id, :id], force: true do |t|
    t.integer :shop_id
    t.integer :id
    t.string :name
  end

  create_table :packages, id: :string, force: true do |t|
    t.integer :version
  end
end
