# frozen_string_literal: true

require "active_record"

require_relative "pluck_in_batches/iterator"
require_relative "pluck_in_batches/extensions"
require_relative "pluck_in_batches/version"

module PluckInBatches
end

ActiveSupport.on_load(:active_record) do
  extend(PluckInBatches::Extensions::ModelExtension)
  ActiveRecord::Relation.include(PluckInBatches::Extensions::RelationExtension)
end
