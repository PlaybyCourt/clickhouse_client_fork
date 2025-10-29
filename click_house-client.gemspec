# frozen_string_literal: true

require_relative "lib/click_house/client/version"

Gem::Specification.new do |spec|
  spec.name = "click_house-client"
  spec.version = ClickHouse::Client::VERSION
  spec.authors = ["group::optimize"]
  spec.email = ["engineering@gitlab.com"]

  spec.summary = "GitLab's client to interact with ClickHouse"
  spec.description = "This Gem provides a simple way to query ClickHouse databases using the HTTP interface."
  spec.homepage = "https://gitlab.com/gitlab-org/ruby/gems/clickhouse-client"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1"

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end

  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "activerecord", ">= 6.1", "< 9.0"
  spec.add_runtime_dependency "activesupport", ">= 6.1", "< 9.0"
  spec.add_runtime_dependency "addressable", "~> 2.8"
  spec.add_runtime_dependency "json", "~> 2.7"

  spec.add_development_dependency "byebug"
  spec.add_development_dependency "gitlab-styles", "~> 12.0.1"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "rubocop"
  spec.add_development_dependency "rubocop-rspec"
end
