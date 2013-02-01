Gem::Specification.new do |gem|
  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = 'kirbybase'
  gem.require_paths = ['lib']
  gem.version       = '3.0.0.dev'

  gem.required_ruby_version = '>= 1.9.2'

  gem.add_runtime_dependency 'striuct', '~> 0.4.2'
  gem.add_runtime_dependency 'validation', '~> 0.0.3'
  gem.add_runtime_dependency 'optionalargument', '~> 0.0.3'
  gem.add_runtime_dependency 'keyvalidatable', '~> 0.0.5'

  gem.add_development_dependency 'yard', '~> 0.8'
  gem.add_development_dependency 'rake', '>= 9'
  gem.add_development_dependency 'bundler', '>= 1.2', '<= 2'
end