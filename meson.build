project('impuls')

py = import('python').find_installation(pure: false)

py.install_sources(
    'impuls/extern/__init__.py',
    'impuls/model/meta/__init__.py',
    'impuls/model/meta/entity.py',
    'impuls/model/meta/gtfs_builder.py',
    'impuls/model/meta/sql_builder.py',
    'impuls/model/meta/utility_types.py',
    'impuls/model/__init__.py',
    'impuls/model/agency.py',
    'impuls/model/attribution.py',
    'impuls/model/calendar_exception.py',
    'impuls/model/calendar.py',
    'impuls/model/fare_attribute.py',
    'impuls/model/fare_rule.py',
    'impuls/model/feed_info.py',
    'impuls/model/frequency.py',
    'impuls/model/route.py',
    'impuls/model/shape_point.py',
    'impuls/model/stop.py',
    'impuls/model/stop_time.py',
    'impuls/model/transfer.py',
    'impuls/model/trip.py',
    'impuls/tasks/__init__.py',
    'impuls/tasks/add_entity.py',
    'impuls/tasks/exec_sql.py',
    'impuls/tasks/generate_trip_headsign.py',
    'impuls/tasks/load_busman.py',
    'impuls/tasks/load_gtfs.py',
    'impuls/tasks/merge.py',
    'impuls/tasks/modify_from_csv.py',
    'impuls/tasks/remove_unused_entities.py',
    'impuls/tasks/save_db.py',
    'impuls/tasks/save_gtfs.py',
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
    'impuls/resource.py',
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

zig_extra_args = []
if meson.is_cross_build()
    zig_extra_args += ['-target', meson.get_external_property('zig_target')]
endif

if get_option('debug')
    zig_extra_args += '-fno-strip'
else
    zig_extra_args += '-fstrip'
endif

if get_option('optimization') == 's'
    zig_extra_args += ['-O', 'ReleaseSmall']
elif get_option('optimization') == '3'
    zig_extra_args += ['-O', 'ReleaseFast']
elif get_option('optimization') == '2'
    zig_extra_args += ['-O', 'ReleaseSafe']
else
    zig_extra_args += ['-O', 'Debug']
endif


zig_wrapper = files('zig_build_lib_wrapper.py')
custom_target(
    'libextern',
    input: [
        # The following inputs are actually passed to zig build-lib
        'impuls/extern/zig/lib.zig',
        # The rest only server as a trigger for meson to rebuild the library
        'impuls/extern/zig/busman.zig',
        'impuls/extern/zig/gtfs.zig',
    ],
    output: lib_name,
    command: [
        py,
        zig_wrapper,
        '-o',
        '@OUTPUT@',
        '--',
        zig_extra_args,
        '-dynamic',
        '--name',
        'extern',
        '-lc',
        # '-I',
        # meson.project_source_root() / 'impuls' / 'extern' / 'c',
        '@INPUT0@',
    ],
    install: true,
    install_dir: py.get_install_dir() / 'impuls' / 'extern',
)
