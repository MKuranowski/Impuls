// © Copyright 2022-2024 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const c = @import("./conversion.zig");
const csv = @import("../csv.zig");
const logging = @import("../logging.zig");
const std = @import("std");
const sqlite3 = @import("../sqlite3.zig");
const t = @import("./table.zig");

const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;
const ColumnValue = c.ColumnValue;
const fs = std.fs;
const Logger = logging.Logger;
const StringHashMapUnmanaged = std.StringArrayHashMapUnmanaged;
const span = std.mem.span;
const Table = t.Table;
const tables = t.tables;
const Thread = std.Thread;

comptime {
    // C guarantees that sizeof(char) is 1 byte, but doesn't guarante that one byte is exactly
    // 8 bits. Platforms with non-8-bit-bytes exist, but are extremely uncommon. As this module
    // treats c_char and u8 interchangebly, crash if those types have different sizes.
    if (@typeInfo(c_char).Int.bits != @typeInfo(u8).Int.bits)
        @compileError("u8 and c_char have different widths. This module expectes those types to be interchangable.");
}

/// Non-null pointer to a null-terminated C byte string, aka `char const*`.
pub const c_char_p = [*:0]const u8;

/// Non-null pointer to a null-terminated vector of c_strings, aka `char const* const*`.
pub const c_char_p_p = [*:null]const ?c_char_p;

/// Headers represents the requested fields to save when exporting GTFS data.
pub const Headers = extern struct {
    agency: ?c_char_p_p = null,
    attributions: ?c_char_p_p = null,
    calendar: ?c_char_p_p = null,
    calendar_dates: ?c_char_p_p = null,
    feed_info: ?c_char_p_p = null,
    routes: ?c_char_p_p = null,
    stops: ?c_char_p_p = null,
    shapes: ?c_char_p_p = null,
    trips: ?c_char_p_p = null,
    stop_times: ?c_char_p_p = null,
    frequencies: ?c_char_p_p = null,
    transfers: ?c_char_p_p = null,
    fare_attributes: ?c_char_p_p = null,
    fare_rules: ?c_char_p_p = null,
    translations: ?c_char_p_p = null,
};

pub fn save(
    logger: Logger,
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    headers: *Headers,
    emit_empty_calendars: bool,
) !void {
    var gtfs_dir = try fs.cwd().openDirZ(gtfs_dir_path, .{});
    defer gtfs_dir.close();

    var threads: [tables.len]?Thread = [_]?Thread{null} ** tables.len;
    defer {
        for (threads) |maybe_thread| {
            if (maybe_thread) |thread| {
                thread.join();
            }
        }
    }

    var wg = Thread.WaitGroup{};
    var failed = Atomic(bool).init(false);

    inline for (tables, 0..) |table, i| {
        const maybe_header: ?c_char_p_p = @field(headers, table.gtfsNameWithoutExtension());
        if (maybe_header) |header| {
            wg.start();
            threads[i] = try Thread.spawn(
                .{},
                TableSaverFile(table).saveInThread,
                .{
                    logger,
                    gtfs_dir,
                    db_path,
                    header,
                    emit_empty_calendars,
                    &wg,
                    &failed,
                },
            );
        }
    }

    wg.wait();
    return if (failed.load(.monotonic)) error.ThreadFailed else {};
}

/// Column describes how to retrieve a field of a single record
const Column = union(enum) {
    /// standard represents a normal Column, present in the `Table.columns`
    /// under the provided index. Custom conversions may apply.
    standard: usize,

    /// extra represents an extra Column, present in `extra_fields_json`
    /// under the provided key. Used only if `Table.has_extra_fields_json` is set.
    extra: []const u8,

    /// none represents a Column which can't be found in an SQL record.
    /// Used only if `Table.has_extra_fields_json` is not set.
    none,
};

/// TableSaver saves GTFS data from a given SQL table to a writer.
fn TableSaver(comptime table: Table, comptime io_writer: type) type {
    const ColumnsBuffer = std.BoundedArray(Column, 32);
    const column_by_gtfs_name = comptime table.gtfsColumnNamesToIndices();
    const is_calendars = comptime std.mem.eql(u8, table.sql_name, "calendars");

    return struct {
        const Self = @This();

        allocator: Allocator,

        /// columns is the order of fields to write for each record
        columns: ColumnsBuffer,

        /// select is a compiled SQL "SELECT ... FROM table.sql_name" statement, retrieving
        /// all SQL columns
        select: sqlite3.Statement,

        /// writer writes CSV data to a buffered fs.File
        writer: csv.Writer(io_writer),

        /// extra_fields_columns is an index of the extra_fields_column in the SELECT statement,
        /// if required and present.
        extra_fields_column: ?c_int = null,

        /// init creates a TableServer writing GTFS data from a provided database
        /// to a provided writer with the provided columns.
        fn init(db: sqlite3.Connection, writer: io_writer, header: []const c_char_p, allocator: Allocator) !Self {
            var columns = ColumnsBuffer{};
            var loads_extra_fields = false;
            for (header) |gtfs_column_name| {
                if (column_by_gtfs_name.get(span(gtfs_column_name))) |column_idx| {
                    try columns.append(Column{ .standard = column_idx });
                } else if (table.has_extra_fields_json) {
                    loads_extra_fields = true;
                    try columns.append(Column{ .extra = span(gtfs_column_name) });
                } else {
                    try columns.append(Column{ .none = {} });
                }
            }

            const column_names = comptime table.columnNames();
            const select_sql = if (loads_extra_fields)
                "SELECT " ++ column_names ++ ", extra_fields_json FROM " ++ table.sql_name
            else
                "SELECT " ++ column_names ++ " FROM " ++ table.sql_name;
            var select = try db.prepare(select_sql);
            errdefer select.deinit();

            return .{
                .allocator = allocator,
                .columns = columns,
                .select = select,
                .writer = csv.writer(writer),
                .extra_fields_column = if (loads_extra_fields) @intCast(table.columns.len) else null,
            };
        }

        /// deinit deallocates any resources allocated by the TableServer.
        fn deinit(self: Self) void {
            self.select.deinit();
        }

        /// writeHeader rewrites the provided header to the underlying file.
        fn writeHeader(self: *Self, header: []const c_char_p) !void {
            for (header) |gtfs_column_name| {
                try self.writer.writeField(std.mem.span(gtfs_column_name));
            }
            try self.writer.terminateRecord();
        }

        /// writeRows rewrites all rows from SQL to GTFS. If emit_empty_calendars is true,
        /// entities from calendar.txt with all weekdays set to zero will be omitted.
        fn writeRows(self: *Self, emit_empty_calendars: bool) !void {
            while (try self.select.step()) {
                try self.writeRow(emit_empty_calendars);
            }
        }

        /// writeRows rewrites a row from SQL to GTFS - should be called only after
        /// select.step() returns true. If emit_empty_calendars is true,
        /// entities from calendar.txt with all weekdays set to zero will be omitted.
        fn writeRow(self: *Self, emit_empty_calendars: bool) !void {
            if (is_calendars and !emit_empty_calendars and isCalendarEmpty(self.select))
                return;

            var extra_fields: ?OwnedExtraFields = null;
            defer if (extra_fields) |*ef| ef.deinit();
            if (self.extra_fields_column) |extra_fields_column| {
                var column_data: ?[]const u8 = undefined;
                extra_fields = OwnedExtraFields.init(self.allocator);
                self.select.column(extra_fields_column, &column_data);
                try extra_fields.?.parse(column_data);
            }

            for (self.columns.slice()) |column| {
                switch (column) {
                    .standard => |column_idx| {
                        var value = ColumnValue.scan(self.select, @intCast(column_idx));
                        if (table.columns[column_idx].to_gtfs) |converter| converter(&value);
                        try self.writer.writeField(try value.ensureString());
                    },
                    .extra => |extra_col_name| {
                        try self.writer.writeField(extra_fields.?.get(extra_col_name));
                    },
                    .none => {
                        try self.writer.writeField("");
                    },
                }
            }
            try self.writer.terminateRecord();
        }

        /// save rewrites data from an Impuls table from a database file to the corresponding
        /// GTFS file in the provided directory, using the provided header. If emit_empty_calendars
        /// is true, entities from calendar.txt with all weekdays set to zero will be omitted.
        fn save(
            allocator: Allocator,
            gtfs_dir: fs.Dir,
            db_path: [*:0]const u8,
            c_header: c_char_p_p,
            emit_empty_calendars: bool,
        ) !void {
            var db = try sqlite3.Connection.init(
                db_path,
                .{ .mode = .read_only, .threading_mode = .no_mutex },
            );
            defer db.deinit();

            var file = try gtfs_dir.createFileZ(table.gtfs_name, .{});
            defer file.close();

            var buffer = bufferedWriterSize(8192, file.writer());

            const header = sliceOverCStrings(c_header);
            var saver = try Self.init(db, buffer.writer(), header, allocator);
            defer saver.deinit();

            try saver.writeHeader(header);
            try saver.writeRows(emit_empty_calendars);

            try buffer.flush();
        }

        /// saveInThread rewrites data from an Impuls table from a database file to the
        /// corresponding GTFS file in the provided directory, using the provided header.
        /// If emit_empty_calendars is true, entities from calendar.txt with all weekdays set to
        /// zero will be omitted.
        ///
        /// This method simply calls save, and if that fails - prints error details to the stderr,
        /// and sets the failure flag. If save succeedes, failure is left as-is. wg.finish() is
        /// always called on exit.
        fn saveInThread(
            logger: Logger,
            gtfs_dir: fs.Dir,
            db_path: [*:0]const u8,
            header: c_char_p_p,
            emit_empty_calendars: bool,
            wg: *Thread.WaitGroup,
            failure: *Atomic(bool),
        ) void {
            var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
            defer _ = gpa.deinit();

            defer wg.finish();
            Self.save(gpa.allocator(), gtfs_dir, db_path, header, emit_empty_calendars) catch |err| {
                failure.store(true, .release);

                if (@errorReturnTrace()) |trace| {
                    logger.err(
                        "gtfs.save: {s}: {}\nStack trace: {}",
                        .{ table.gtfs_name, err, trace },
                    );
                } else {
                    logger.err("gtfs.save: {s}: {}", .{ table.gtfs_name, err });
                }
                return;
            };
            logger.debug("Saving " ++ table.gtfs_name ++ " completed", .{});
        }
    };
}

fn TableSaverFile(comptime table: Table) type {
    const io_writer = std.io.BufferedWriter(8192, fs.File.Writer).Writer;
    return TableSaver(table, io_writer);
}

/// OwnedExtraFields represents a parsed `extra_fields_json` field.
const OwnedExtraFields = struct {
    arena: std.heap.ArenaAllocator,
    map: std.StringArrayHashMapUnmanaged([]const u8) = .{},

    fn init(allocator: Allocator) OwnedExtraFields {
        return .{ .arena = std.heap.ArenaAllocator.init(allocator) };
    }

    fn deinit(self: *OwnedExtraFields) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn parse(self: *OwnedExtraFields, raw_value: ?[]const u8) !void {
        if (raw_value) |json_string| {
            self.map = (try std.json.parseFromSliceLeaky(
                std.json.ArrayHashMap([]const u8),
                self.arena.allocator(),
                json_string,
                .{},
            )).map;
        }
    }

    fn get(self: *const OwnedExtraFields, k: []const u8) []const u8 {
        return self.map.get(k) orelse "";
    }
};

/// bufferedWriterSize creates a std.io.BufferedWriter over the provided stream with a given
/// size buffer.
fn bufferedWriterSize(
    comptime size: usize,
    underlying_stream: anytype,
) std.io.BufferedWriter(size, @TypeOf(underlying_stream)) {
    return .{ .unbuffered_writer = underlying_stream };
}

/// sliceOverCStrings converts a c_char_p_p to a slice of *non-optional* c_char_p.
///
/// This is in contrast to `std.mem.span`, which would return a slice of *optional* c_char_p.
fn sliceOverCStrings(ptr: c_char_p_p) []const c_char_p {
    return @as([*]const c_char_p, @ptrCast(ptr))[0..std.mem.len(ptr)];
}

/// isCalendarEmpty returns true if none of the weekdays of a calendar are set to zero.
///
/// select must be a "SELECT xxx FROM calendars" statement pointing to a valid row,
/// with the 2nd column representing Monday, 3rd - Tuesday, etc. up tu 8th - Sunday.
fn isCalendarEmpty(select: sqlite3.Statement) bool {
    // XXX: The columns must be in the following order:
    // (ignored),monday,tuesday,wednesday,thursday,friday,saturday,sunday
    for (1..8) |idx| {
        var active: bool = undefined;
        select.column(@intCast(idx), &active);
        if (active) return false;
    }
    return true;
}

test "gtfs.save.simple" {
    var out_dir = std.testing.tmpDir(.{});
    defer out_dir.cleanup();

    const header_slice = &[_]?c_char_p{ "agency_id", "route_id", "route_short_name", "route_long_name", "route_type", null };
    const header: c_char_p_p = header_slice[0 .. header_slice.len - 1 :null];

    try TableSaverFile(t.tables[5]).save(
        std.testing.allocator,
        out_dir.dir,
        "tests/tasks/fixtures/wkd.db",
        header,
        false,
    );

    const content = try out_dir.dir.readFileAlloc(std.testing.allocator, "routes.txt", 16384);
    defer std.testing.allocator.free(content);

    try std.testing.expectEqualStrings(
        // zig fmt: off
        "agency_id,route_id,route_short_name,route_long_name,route_type\r\n"
        ++ "0,A1,A1,Warszawa Śródmieście WKD — Grodzisk Mazowiecki Radońska,2\r\n"
        ++ "0,ZA1,ZA1,Podkowa Leśna Główna — Grodzisk Mazowiecki Radońska (ZKA),3\r\n"
        ++ "0,ZA12,ZA12,Podkowa Leśna Główna — Milanówek Grudów (ZKA),3\r\n",
        // zig fmt: on
        content,
    );
}

test "gtfs.save.extra_fields" {
    var db = try sqlite3.Connection.init(":memory:", .{});
    defer db.deinit();

    try db.exec(
        \\CREATE TABLE agencies (
        \\    agency_id TEXT PRIMARY KEY,
        \\    name TEXT NOT NULL,
        \\    url TEXT NOT NULL,
        \\    timezone TEXT NOT NULL,
        \\    lang TEXT NOT NULL DEFAULT '',
        \\    phone TEXT NOT NULL DEFAULT '',
        \\    fare_url TEXT NOT NULL DEFAULT '',
        \\    extra_fields_json TEXT
        \\) STRICT;
    );
    try db.exec(
        \\ INSERT INTO agencies VALUES ('0', 'Foo', 'https://example.com', 'UTC',
        \\ 'en', '', '', '{"agency_email":"foo@example.com"}')
    );
    try db.exec(
        \\ INSERT INTO agencies VALUES ('1', 'Bar', 'https://example.com', 'UTC',
        \\ 'en', '', '', NULL)
    );

    const header: []const c_char_p = &.{ "agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_email" };

    var b: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&b);
    const writer = fbs.writer();

    var s = try TableSaver(t.tables[0], @TypeOf(writer)).init(
        db,
        writer,
        header,
        std.testing.allocator,
    );
    defer s.deinit();

    try s.writeHeader(header);
    try s.writeRows(false);

    const content = b[0..fbs.pos];
    try std.testing.expectEqualStrings(
        // zig fmt: off
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_email\r\n"
        ++ "0,Foo,https://example.com,UTC,en,foo@example.com\r\n"
        ++ "1,Bar,https://example.com,UTC,en,\r\n",
        // zig fmt: on
        content,
    );
}
