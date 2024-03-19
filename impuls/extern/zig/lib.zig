const std = @import("std");
const print = std.debug.print;

const gtfs = @import("./gtfs/lib.zig");

// XXX: Function declarations from this file must match ../__init__.py

pub export fn load_gtfs(
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
) c_int {
    gtfs.load(db_path, gtfs_dir_path) catch |err| {
        if (@errorReturnTrace()) |trace| std.debug.dumpStackTrace(trace.*);
        std.debug.print("{}\n", .{err});
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
        if (@errorReturnTrace()) |trace| std.debug.dumpStackTrace(trace.*);
        std.debug.print("{}\n", .{err});
        return 1;
    };
    return 0;
}
