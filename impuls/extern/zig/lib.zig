const std = @import("std");
const print = std.debug.print;

const busman = @import("./busman.zig");
const gtfs = @import("./gtfs.zig");

// XXX: Function declarations from this file must match ../__init__.py

pub export fn load_busman(
    db_path: [*:0]const u8,
    mdb_path: [*:0]const u8,
    agency_id: [*:0]const u8,
    ignore_route_id: bool,
    ignore_stop_id: bool,
) c_int {
    busman.load(db_path, mdb_path, agency_id, ignore_route_id, ignore_stop_id) catch |err| {
        if (@errorReturnTrace()) |trace| std.debug.dumpStackTrace(trace.*);
        std.debug.print("{}\n", .{err});
        return 1;
    };
    return 0;
}

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
