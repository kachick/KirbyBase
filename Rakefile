#!/usr/bin/env rake
require 'bundler/gem_tasks'

require 'rake/testtask'

task default: [:test]

Rake::TestTask.new do |tt|
  tt.verbose = true
  tt.warning = false # warning will be enabled in test/helper
end