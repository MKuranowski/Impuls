// © Copyright 2022-2024 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const std = @import("std");

const gtfs = @import("./gtfs/lib.zig");
const logging = @import("./logging.zig");

pub const std_options = .{
    .log_level = .debug,
    .logFn = logging.logWithCustomHandler,
};

// XXX: Function declarations from this file must match ../__init__.py

pub export fn set_log_handler(handler: ?logging.Handler) void {
    std.debug.assert(std.options.logFn == std_options.logFn);
    logging.custom_handler = handler;
}

pub export fn load_gtfs(
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    extra_fields: bool,
    extra_files_ptr: [*]const [*:0]const u8,
    extra_files_len: c_uint,
) c_int {
    const extra_files = extra_files_ptr[0..extra_files_len];
    gtfs.load(db_path, gtfs_dir_path, extra_fields, extra_files) catch |err| {
        if (@errorReturnTrace()) |trace| {
            std.log.err("gtfs.load: {}\nStack trace: {}", .{ err, trace });
        } else {
            std.log.err("gtfs.load: {}", .{err});
        }
        return 1;
    };
    return 0;
}

pub const GTFSHeaders = gtfs.Headers;

pub export fn save_gtfs(
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    headers: *GTFSHeaders,
    emit_empty_calendars: bool,
) c_int {
    gtfs.save(db_path, gtfs_dir_path, headers, emit_empty_calendars) catch |err| {
        if (@errorReturnTrace()) |trace| {
            std.log.err("gtfs.save: {}\nStack trace: {}", .{ err, trace });
        } else {
            std.log.err("gtfs.save: {}", .{err});
        }
        return 1;
    };
    return 0;
}
