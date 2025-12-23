# © Copyright 2022-2025 Mikołaj Kuranowski
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

zig_lib_args = ['-dynamic', '--name', 'extern']
if meson.is_cross_build()
    zig_lib_args += ['-target', meson.get_external_property('zig_target')]
endif

if get_option('debug')
    zig_lib_args += ['-fno-strip', '-fno-omit-frame-pointer']
else
    zig_lib_args += '-fstrip'
endif

if get_option('optimization') == 's'
    zig_lib_args += ['-O', 'ReleaseSmall']
elif get_option('optimization') == '3'
    zig_lib_args += ['-O', 'ReleaseFast']
elif get_option('optimization') == '2'
    zig_lib_args += ['-O', 'ReleaseSafe']
else
    zig_lib_args += ['-O', 'Debug']
endif

zig_compile_args = [
    '-lc',
    '-I',
    meson.project_source_root() / 'impuls' / 'extern' / 'vendor',
    # https://sqlite.org/compile.html#recommended_compile_time_options
    '-DSQLITE_DQS=0',
    '-DSQLITE_DEFAULT_MEMSTATUS=0',
    '-DSQLITE_LIKE_DOESNT_MATCH_BLOBS',
    '-DSQLITE_MAX_EXPR_DEPTH=0',
    '-DSQLITE_OMIT_DECLTYPE',
    '-DSQLITE_OMIT_DEPRECATED',
    '-DSQLITE_OMIT_PROGRESS_CALLBACK',
    '-DSQLITE_OMIT_SHARED_CACHE',
    '-DSQLITE_USE_ALLOCA',
]


zig_wrapper = files('zig_build_lib_wrapper.py')
custom_target(
    'libextern',
    input: [
        # The following inputs are actually passed to zig build-lib
        'impuls/extern/zig/root.zig',
        'impuls/extern/vendor/sqlite3.c',
        # The rest only serve as a trigger for meson to rebuild the library
        'impuls/extern/zig/gtfs/conversion.zig',
        'impuls/extern/zig/gtfs/conversion_from_gtfs.zig',
        'impuls/extern/zig/gtfs/conversion_to_gtfs.zig',
        'impuls/extern/zig/gtfs/root.zig',
        'impuls/extern/zig/gtfs/load.zig',
        'impuls/extern/zig/gtfs/save.zig',
        'impuls/extern/zig/gtfs/table.zig',
        'impuls/extern/zig/bounded_array.zig',
        'impuls/extern/zig/csv.zig',
        'impuls/extern/zig/logging.zig',
        'impuls/extern/zig/sqlite3.zig',
    ],
    output: lib_name,
    command: [
        py,
        zig_wrapper,
        '-o',
        '@OUTPUT@',
        '--',
        zig_lib_args,
        zig_compile_args,
        '@INPUT0@',
        '@INPUT1@',
    ],
    install: true,
    install_dir: py.get_install_dir() / 'impuls' / 'extern',
)

zig = find_program('zig')
test(
    'libextern-test',
    zig,
    args: [
        'test',
        zig_compile_args,
        files(
            'impuls/extern/zig/root.zig',
            'impuls/extern/vendor/sqlite3.c',
        ),
    ],
    verbose: true,
    workdir: meson.project_source_root(),
)
