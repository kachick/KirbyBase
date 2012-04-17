# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{KirbyBase}
  s.version = "2.6.1"

  s.required_rubygems_version = nil if s.respond_to? :required_rubygems_version=
  s.authors = ["Jamey Cribbs","gurugeek"]
  s.autorequire = %q{kirbybase}
  s.date = %q{2012-04-17}
  s.default_executable = %q{kbserver.rb}
  s.description = %q{KirbyBase is a class that allows you to create and manipulate simple, plain-text databases. You can use it in either a single-user or client-server mode. You can select records for retrieval/updating using code blocks.}
  s.email = %q{jcribbs@netpromi.com}
  s.executables = ["kbserver.rb"]
  s.files = ["lib/kirbybase.rb", "bin/kbserver.rb", "README", "changes.txt", "kirbybaserubymanual.html", "test/base_test.rb", "test/tc_local_db.rb", "test/tc_local_table.rb", "test/ts_local.rb", "examples/aaa_try_this_first", "examples/add_column_test", "examples/calculated_field_test", "examples/change_column_type_test", "examples/column_required_test", "examples/crosstab_test", "examples/csv_import_test", "examples/default_value_test", "examples/drop_column_test", "examples/indexes_test", "examples/kbserver_as_win32_service", "examples/link_many_test", "examples/lookup_field_test", "examples/many_to_many_test", "examples/memo_test", "examples/record_class_test", "examples/rename_column_test", "examples/rename_table_test", "examples/yaml_field_test", "examples/aaa_try_this_first/kbtest.rb", "examples/add_column_test/add_column_test.rb", "examples/calculated_field_test/calculated_field_test.rb", "examples/change_column_type_test/change_column_type_test.rb", "examples/column_required_test/column_required_test.rb", "examples/crosstab_test/crosstab_test.rb", "examples/csv_import_test/csv_import_test.rb", "examples/csv_import_test/plane.csv", "examples/default_value_test/default_value_test.rb", "examples/drop_column_test/drop_column_test.rb", "examples/indexes_test/add_index_test.rb", "examples/indexes_test/drop_index_test.rb", "examples/indexes_test/index_test.rb", "examples/kbserver_as_win32_service/kbserverctl.rb", "examples/kbserver_as_win32_service/kbserver_daemon.rb", "examples/link_many_test/link_many_test.rb", "examples/lookup_field_test/lookup_field_test.rb", "examples/lookup_field_test/lookup_field_test_2.rb", "examples/lookup_field_test/the_hal_fulton_feature_test.rb", "examples/many_to_many_test/many_to_many_test.rb", "examples/memo_test/memos", "examples/memo_test/memo_test.rb", "examples/memo_test/memos/blank.txt", "examples/record_class_test/record_class_test.rb", "examples/record_class_test/record_class_test2.rb", "examples/rename_column_test/rename_column_test.rb", "examples/rename_table_test/rename_table_test.rb", "examples/yaml_field_test/yaml_field_test.rb", "images/blank.png", "images/callouts", "images/caution.png", "images/client_server.png", "images/example.png", "images/home.png", "images/important.png", "images/kirby1.jpg", "images/next.png", "images/note.png", "images/prev.png", "images/single_user.png", "images/smallnew.png", "images/tip.png", "images/toc-blank.png", "images/toc-minus.png", "images/toc-plus.png", "images/up.png", "images/warning.png", "images/callouts/1.png", "images/callouts/10.png", "images/callouts/11.png", "images/callouts/12.png", "images/callouts/13.png", "images/callouts/14.png", "images/callouts/15.png", "images/callouts/2.png", "images/callouts/3.png", "images/callouts/4.png", "images/callouts/5.png", "images/callouts/6.png", "images/callouts/7.png", "images/callouts/8.png", "images/callouts/9.png"]
  s.homepage = %q{http://www.netpromi.com/kirbybase_ruby.html}
  s.require_paths = ["lib"]
  s.required_ruby_version = Gem::Requirement.new("> 0.0.0")
  s.requirements = ["none"]
  s.rubyforge_project = %q{kirbybase}
  s.rubygems_version = %q{1.3.6}
  s.summary = %q{KirbyBase is a simple, pure-Ruby, plain-text, flat-file database management system.}
  s.test_files = ["test/ts_local.rb"]

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 1

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
