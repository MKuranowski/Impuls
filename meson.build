# © Copyright 2022-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

project('impuls')

py = import('python').find_installation(pure: false)

py.install_sources(
    'impuls/extern/__init__.py',
    'impuls/model/meta/__init__.py',
    'impuls/model/meta/entity.py',
    'impuls/model/meta/extra_fields_mixin.py',
    'impuls/model/meta/sql_builder.py',
    'impuls/model/meta/utility_types.py',
    'impuls/model/__init__.py',
    'impuls/model/agency.py',
    'impuls/model/attribution.py',
    'impuls/model/calendar_exception.py',
    'impuls/model/calendar.py',
    'impuls/model/extra_table_row.py',
    'impuls/model/fare_attribute.py',
    'impuls/model/fare_rule.py',
    'impuls/model/feed_info.py',
    'impuls/model/frequency.py',
    'impuls/model/route.py',
    'impuls/model/shape_point.py',
    'impuls/model/stop.py',
    'impuls/model/stop_time.py',
    'impuls/model/transfer.py',
    'impuls/model/translation.py',
    'impuls/model/trip.py',
    'impuls/tasks/__init__.py',
    'impuls/tasks/add_entity.py',
    'impuls/tasks/assign_directions.py',
    'impuls/tasks/exec_sql.py',
    'impuls/tasks/extend_calendars.py',
    'impuls/tasks/generate_trip_headsign.py',
    'impuls/tasks/load_busman.py',
    'impuls/tasks/load_db.py',
    'impuls/tasks/load_gtfs.py',
    'impuls/tasks/merge.py',
    'impuls/tasks/modify_from_csv.py',
    'impuls/tasks/remove_unused_entities.py',
    'impuls/tasks/save_db.py',
    'impuls/tasks/save_gtfs.py',
    'impuls/tasks/simplify_calendars.py',
    'impuls/tasks/split_trip_legs.py',
    'impuls/tasks/truncate_calendars.py',
    'impuls/tools/__init__.py',
    'impuls/tools/color.py',
    'impuls/tools/geo.py',
    'impuls/tools/iteration.py',
    'impuls/tools/logs.py',
    'impuls/tools/machine_load.py',
    'impuls/tools/polish_calendar_exceptions.py',
    'impuls/tools/strings.py',
    'impuls/tools/temporal.py',
    'impuls/tools/testing_mocks.py',
    'impuls/tools/types.py',
    'impuls/__init__.py',
    'impuls/app.py',
    'impuls/db.py',
    'impuls/errors.py',
    'impuls/multi_file.py',
    'impuls/options.py',
    'impuls/pipeline.py',
    'impuls/py.typed',
    'impuls/resource.py',
    'impuls/selector.py',
    'impuls/task.py',
    preserve_path: true,
)

if host_machine.system() == 'windows'
    lib_suffix = '.dll'
elif host_machine.system() == 'darwin'
    lib_suffix = '.dylib'
elif host_machine.system() == 'linux'
    lib_suffix = '.so'
else
    warning('Unknown target system: ', host_machine.system(), ' - assuming shared dynamic libraries use \'.so\' suffix')
    lib_suffix = '.so'
endif
lib_name = 'libextern' + lib_suffix

cargo_wrapper_args = []
if meson.is_cross_build()
    cargo_wrapper_args += ['--cross', meson.get_external_property('cargo_target')]
endif

cargo_args = []
if get_option('optimization') in ['s', '2', '3']
    cargo_args += ['--release']
endif

cargo_wrapper = files('cargo_build_wrapper.py')
custom_target(
    'libextern',
    input: [
        # This list only serves as a trigger for rebuilding the library
        'impuls/extern/Cargo.lock',
        'impuls/extern/Cargo.toml',
        'impuls/extern/db.rs',
        'impuls/extern/error.rs',
        'impuls/extern/gtfs/load.rs',
        'impuls/extern/gtfs/mod.rs',
        'impuls/extern/gtfs/save.rs',
        'impuls/extern/gtfs/schema.rs',
        'impuls/extern/gtfs/table.rs',
        'impuls/extern/lib.rs',
        'impuls/extern/logging.rs',
    ],
    output: lib_name,
    command: [
        py,
        cargo_wrapper,
        '--output',
        '@OUTPUT@',
        '--source-dir',
        '@CURRENT_SOURCE_DIR@/impuls/extern',
        '--target-dir',
        '@PRIVATE_DIR@',
        cargo_wrapper_args,
        '--',
        cargo_args,
    ],
    install: true,
    install_dir: py.get_install_dir() / 'impuls' / 'extern',
)

cargo = find_program('cargo')
test(
    'libextern-test',
    cargo,
    args: ['test'],
    verbose: true,
    workdir: meson.project_source_root() / 'impuls' / 'extern',
)
