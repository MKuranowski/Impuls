const std = @import("std");

const gtfs = @import("./gtfs/lib.zig");
const logging = @import("./logging.zig");

// XXX: Function declarations from this file must match ../__init__.py

pub const LogHandler = logging.Handler;

pub export fn load_gtfs(
    log_handler: LogHandler,
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
) c_int {
    const logger = logging.Logger{ .handler = log_handler };
    gtfs.load(logger, db_path, gtfs_dir_path) catch |err| {
        if (@errorReturnTrace()) |trace| {
            logger.err("gtfs.load: {}\nStack trace: {}", .{ err, trace });
        } else {
            logger.err("gtfs.load: {}", .{err});
        }
        return 1;
    };
    return 0;
}

pub const GTFSHeaders = gtfs.Headers;

pub export fn save_gtfs(
    log_handler: LogHandler,
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    headers: *GTFSHeaders,
    emit_empty_calendars: bool,
) c_int {
    const logger = logging.Logger{ .handler = log_handler };
    gtfs.save(logger, db_path, gtfs_dir_path, headers, emit_empty_calendars) catch |err| {
        if (@errorReturnTrace()) |trace| {
            logger.err("gtfs.save: {}\nStack trace: {}", .{ err, trace });
        } else {
            logger.err("gtfs.save: {}", .{err});
        }
        return 1;
    };
    return 0;
}
