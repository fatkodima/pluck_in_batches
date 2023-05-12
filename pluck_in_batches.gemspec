# frozen_string_literal: true

require_relative "lib/pluck_in_batches/version"

Gem::Specification.new do |spec|
  spec.name = "pluck_in_batches"
  spec.version = PluckInBatches::VERSION
  spec.authors = ["fatkodima"]
  spec.email = ["fatkodima123@gmail.com"]

  spec.summary = "Change"
  spec.homepage = "https://github.com/fatkodima/pluck_in_batches"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.7.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/master/CHANGELOG.md"

  spec.files         = Dir["*.{md,txt}", "lib/**/*"]
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord", ">= 6.0"
end
