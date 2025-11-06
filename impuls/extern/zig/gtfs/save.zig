// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const builtin = @import("builtin");
const c = @import("./conversion.zig");
const csv = @import("../csv.zig");
const std = @import("std");
const sqlite3 = @import("../sqlite3.zig");
const t = @import("./table.zig");

const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;
const ColumnMapping = t.ColumnMapping;
const ColumnValue = c.ColumnValue;
const fs = std.fs;
const Table = t.Table;
const tables = t.tables;
const Thread = std.Thread;

comptime {
    // C guarantees that sizeof(char) is 1 byte, but doesn't guarante that one byte is exactly
    // 8 bits. Platforms with non-8-bit-bytes exist, but are extremely uncommon. As this module
    // treats c_char and u8 interchangebly, crash if those types have different sizes.
    if (@typeInfo(c_char).int.bits != @typeInfo(u8).int.bits)
        @compileError("u8 and c_char have different widths. This module expectes those types to be interchangable.");
}

/// Non-null pointer to a null-terminated C byte string, aka `char const*`.
pub const c_char_p = [*:0]const u8;

/// Non-null pointer to a null-terminated vector of c_strings, aka `char const* const*`.
pub const c_char_p_p = [*:null]const ?c_char_p;

pub const FileHeader = extern struct {
    file_name: c_char_p,
    fields: c_char_p_p,
};

pub fn save(
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    headers: []const FileHeader,
    emit_empty_calendars: bool,
    ensure_order: bool,
) !void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer std.debug.assert(gpa.deinit() == .ok);
    // const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;
    const allocator = std.heap.c_allocator;

    var gtfs_dir = try fs.cwd().openDirZ(gtfs_dir_path, .{});
    defer gtfs_dir.close();

    var threads = try std.ArrayList(Thread).initCapacity(allocator, headers.len);
    defer {
        for (threads.items) |thread| {
            thread.join();
        }
        threads.deinit(allocator);
    }

    var wg = Thread.WaitGroup{};
    var failed = Atomic(bool).init(false);

    for (headers) |header| {
        wg.start();
        const thread = try spawnSaveThread(
            gtfs_dir,
            db_path,
            header,
            emit_empty_calendars,
            ensure_order,
            &wg,
            &failed,
        );
        errdefer thread.join();
        threads.append(allocator, thread) catch std.debug.panic("`threads` should have enough capacity to hold as many threads as there are files to save", .{});
    }

    wg.wait();
    return if (failed.load(.monotonic)) error.ThreadFailed else {};
}

/// spawnSaveThread spawns a new thread calling `saveTableInThread` or `saveExtraTableInThread`.
fn spawnSaveThread(
    gtfs_dir: fs.Dir,
    db_path: [*:0]const u8,
    header: FileHeader,
    emit_empty_calendars: bool,
    ensure_order: bool,
    wg: *Thread.WaitGroup,
    failure: *Atomic(bool),
) !Thread {
    return if (t.tableByGtfsName(std.mem.span(header.file_name))) |table|
        Thread.spawn(
            .{},
            saveTableInThread,
            .{
                gtfs_dir,
                db_path,
                table,
                header.fields,
                emit_empty_calendars,
                ensure_order,
                wg,
                failure,
            },
        )
    else
        try Thread.spawn(
            .{},
            saveExtraTableInThread,
            .{
                gtfs_dir,
                db_path,
                header.file_name,
                header.fields,
                wg,
                failure,
            },
        );
}

/// TableSaver saves GTFS data from a given SQL table to a writer.
const TableSaver = struct {
    allocator: Allocator,

    /// table points to the Table to be exported
    table: *const Table,

    /// columns is the order of fields to write for each record
    columns: []ColumnMapping,

    /// select is a compiled SQL "SELECT ... FROM table.sql_name" statement, retrieving
    /// all SQL columns
    select: sqlite3.Statement,

    /// writer writes CSV data to a buffered fs.File
    writer: csv.Writer,

    /// header contains the field to export
    header: []const [:0]const u8,

    /// extra_fields_columns is an index of the extra_fields_json column in the SELECT
    /// statement, if required and present
    extra_fields_column: ?c_int = null,

    /// filter_empty_calendars flags whether rows for which `isCalendarEmpty` returns true
    /// should not be written to the writer. Can only be set if `table` represents `calendars`.
    filter_empty_calendars: bool = false,

    /// init creates a TableSaver writing GTFS data from a provided database
    /// to a provided writer, according to the provided header.
    ///
    /// If a non-standard field is encountered in `header`, TableSaver will attempt
    /// to access that fields from `extra_fields` (if the `Table` has it), otherwise
    /// that column will always have empty cells written.
    ///
    /// `emit_empty_calendars` is only used if `table.sql_name` is `calendars`. If that
    /// flag is not set, empty calendar rows (inactive on all weekdays) are not going
    /// to be written to the output file.
    fn init(
        db: sqlite3.Connection,
        writer: *std.Io.Writer,
        table: *const Table,
        header: c_char_p_p,
        emit_empty_calendars: bool,
        ensure_order: bool,
        allocator: Allocator,
    ) !TableSaver {
        const header_slice = try ownedSliceOverCStrings(header, allocator);
        errdefer allocator.free(header_slice);

        const columns_and_extra_fields = try prepareColumnMapping(table, header_slice, allocator);
        const columns = columns_and_extra_fields[0];
        const extra_fields = columns_and_extra_fields[1];
        errdefer allocator.free(columns);

        const select = try prepareSelect(db, table, extra_fields, ensure_order, allocator);
        errdefer select.deinit();

        const filter_empty_calendars = !emit_empty_calendars and std.mem.eql(u8, table.sql_name, "calendars");

        return .{
            .allocator = allocator,
            .table = table,
            .columns = columns,
            .select = select,
            .writer = csv.Writer.init(writer),
            .header = header_slice,
            .extra_fields_column = if (extra_fields) @intCast(table.columns.len) else null,
            .filter_empty_calendars = filter_empty_calendars,
        };
    }

    /// prepareColumnMapping combines `table.columns` with the `header` to figure out
    /// the order of columns to be written. The second return value indicates whether
    /// `extra_fields_json` need to be parsed to properly write the data, as there are
    /// extra ColumnMappings being used.
    fn prepareColumnMapping(
        table: *const Table,
        header: []const []const u8,
        allocator: Allocator,
    ) !struct { []ColumnMapping, bool } {
        var loads_extra_fields = false;
        var columns: std.ArrayList(ColumnMapping) = .{};
        defer columns.deinit(allocator);

        for (header) |gtfs_column_name| {
            if (table.gtfsColumnNameToIndex(gtfs_column_name)) |column_idx| {
                try columns.append(allocator, .{ .standard = column_idx });
            } else if (table.has_extra_fields_json) {
                loads_extra_fields = true;
                try columns.append(allocator, .{ .extra = gtfs_column_name });
            } else {
                try columns.append(allocator, .{ .none = {} });
            }
        }

        return .{ try columns.toOwnedSlice(allocator), loads_extra_fields };
    }

    /// prepareSelect prepares and compiles an SQL SELECT statement for the provided
    /// table in the provided database. The SELECT statement selects all columns (as
    /// indicated by `table.columns`) and optionally (if `loads_extra_fields` is set)
    /// `extra_fields_json` (guaranteed last column).
    fn prepareSelect(
        db: sqlite3.Connection,
        table: *const Table,
        loads_extra_fields: bool,
        ensure_order: bool,
        allocator: Allocator,
    ) !sqlite3.Statement {
        var sql: std.ArrayList(u8) = .{};
        defer sql.deinit(allocator);

        try sql.appendSlice(allocator, "SELECT ");
        for (table.columns, 0..) |column, i| {
            if (i != 0) try sql.appendSlice(allocator, ", ");
            try sql.appendSlice(allocator, column.name);
        }

        if (loads_extra_fields) {
            try sql.appendSlice(allocator, ", extra_fields_json");
        }

        try sql.appendSlice(allocator, " FROM ");
        try sql.appendSlice(allocator, table.sql_name);

        if (ensure_order) {
            try sql.appendSlice(allocator, table.order_clause);
        }

        try sql.append(allocator, 0);

        const statement = try db.prepare(sql.items[0 .. sql.items.len - 1 :0]);
        return statement;
    }

    /// deinit deallocates any resources allocated by the TableSaver.
    fn deinit(self: TableSaver) void {
        self.allocator.free(self.header);
        self.allocator.free(self.columns);
        self.select.deinit();
    }

    /// writeAll writes complete data to the underlying file - the header and all rows.
    fn writeAll(self: *TableSaver) !void {
        try self.writeHeader();
        try self.writeRows();
    }

    /// writeHeader writes the header to the underlying file.
    fn writeHeader(self: *TableSaver) !void {
        try self.writer.writeRecord(self.header);
    }

    /// writeRows rewrites all rows from SQL to GTFS.
    fn writeRows(self: *TableSaver) !void {
        while (try self.select.step()) {
            try self.writeRow();
        }
    }

    /// writeRow rewrites a row from SQL to GTFS - should be called only after
    /// `self.select.step` returns true.
    fn writeRow(self: *TableSaver) !void {
        if (self.shouldSkipRow()) return;

        var extra_fields = try self.parseExtraFields();
        defer if (extra_fields) |*ef| ef.deinit();
        try self.writeRecord(extra_fields);
    }

    /// shouldSkipRow returns true if the current row should not be written out.
    fn shouldSkipRow(self: *TableSaver) bool {
        return self.filter_empty_calendars and isCalendarEmpty(self.select);
    }

    /// parseExtraFields parses the `extra_fields_json` column, if it is available.
    fn parseExtraFields(self: *TableSaver) !?OwnedExtraFields {
        if (self.extra_fields_column) |column| {
            var json: ?[]const u8 = undefined;
            self.select.column(column, &json);

            var fields = OwnedExtraFields.init(self.allocator);
            errdefer fields.deinit();
            try fields.parse(json);

            return fields;
        } else {
            return null;
        }
    }

    /// writeRecord writes data to the underlying writer.
    fn writeRecord(self: *TableSaver, extra_fields: ?OwnedExtraFields) !void {
        for (self.columns) |column| {
            try self.writeColumn(column, extra_fields);
        }
        try self.writer.terminateRecord();
    }

    /// writeColumn writes data from the provided column to the underlying writer.
    fn writeColumn(self: *TableSaver, column: ColumnMapping, extra_fields: ?OwnedExtraFields) !void {
        switch (column) {
            .standard => |column_idx| {
                var value = ColumnValue.scan(self.select, @intCast(column_idx));
                if (self.table.columns[column_idx].to_gtfs) |converter| converter(&value);
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
};

/// saveTable opens the necessary resources, initializes a `TableSaver` and calls
/// `TableSaver.writeAll` to fully rewrite an SQL table to GTFS.
fn saveTable(
    allocator: Allocator,
    gtfs_dir: fs.Dir,
    db_path: [*:0]const u8,
    table: *const Table,
    header: c_char_p_p,
    emit_empty_calendars: bool,
    ensure_order: bool,
) !void {
    var db = try sqlite3.Connection.init(
        db_path,
        .{ .mode = .read_only, .threading_mode = .no_mutex },
    );
    defer db.deinit();

    var file_buffer: [8192]u8 = undefined;
    var file = try gtfs_dir.createFileZ(table.gtfs_name, .{});
    defer file.close();
    var writer = file.writer(&file_buffer);

    var saver = try TableSaver.init(
        db,
        &writer.interface,
        table,
        header,
        emit_empty_calendars,
        ensure_order,
        allocator,
    );
    defer saver.deinit();
    try saver.writeAll();
    try writer.interface.flush();
    std.log.debug("Saving {s} completed", .{table.gtfs_name});
}

/// saveTableInThread calls `saveTable` to fully rewrite an SQL table to GTFS.
///
/// This function is meant to be run in a separate thread - a new `Allocator` is automatically
/// created; `wg.finish` is called on exit, and `failure` is set to true if `saveTable` fails.
fn saveTableInThread(
    gtfs_dir: fs.Dir,
    db_path: [*:0]const u8,
    table: *const Table,
    header: c_char_p_p,
    emit_empty_calendars: bool,
    ensure_order: bool,
    wg: *Thread.WaitGroup,
    failure: *Atomic(bool),
) void {
    defer wg.finish();

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer std.debug.assert(gpa.deinit() == .ok);
    // const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;
    const allocator = std.heap.c_allocator;

    saveTable(
        allocator,
        gtfs_dir,
        db_path,
        table,
        header,
        emit_empty_calendars,
        ensure_order,
    ) catch |err| {
        failure.store(true, .release);

        if (@errorReturnTrace()) |trace| {
            std.log.err(
                "gtfs.save: {s}: {}\nStack trace: {f}",
                .{ table.gtfs_name, err, trace },
            );
        } else {
            std.log.err("gtfs.save: {s}: {}", .{ table.gtfs_name, err });
        }
        return;
    };
}

/// ExtraTableSaver saves GTFS data from a table stored in the special `extra_table_rows` SQL table.
const ExtraTableSaver = struct {
    allocator: Allocator,

    /// select is a compiled SQL "SELECT fields_json FROM extra_table_rows ..." statement
    select: sqlite3.Statement,

    /// writer writes CSV data to a buffered fs.File
    writer: csv.Writer,

    /// header contains the field to export
    header: []const [:0]const u8,

    /// file_name contains the name of the extra file to export
    file_name: [*:0]const u8,

    /// init creates an ExtraTableSaver writing GTFS data from a provided database
    /// to a provided writer, according to the provided header.
    fn init(
        db: sqlite3.Connection,
        writer: *std.Io.Writer,
        file_name: [*:0]const u8,
        header: c_char_p_p,
        allocator: Allocator,
    ) !ExtraTableSaver {
        const header_slice = try ownedSliceOverCStrings(header, allocator);
        errdefer allocator.free(header_slice);

        var select = try db.prepare(
            \\ SELECT fields_json FROM extra_table_rows
            \\ WHERE table_name = ?
            \\ ORDER BY row_sort_order ASC
        );
        errdefer select.deinit();

        return .{
            .allocator = allocator,
            .select = select,
            .writer = csv.Writer.init(writer),
            .header = header_slice,
            .file_name = file_name,
        };
    }

    /// deinit deallocates any resources allocated by the ExtraTableSaver.
    fn deinit(self: *ExtraTableSaver) void {
        self.allocator.free(self.header);
        self.select.deinit();
    }

    /// writeAll writes complete data to the underlying file - the header and all rows.
    fn writeAll(self: *ExtraTableSaver) !void {
        try self.writeHeader();
        try self.bindSelectArguments();
        while (try self.select.step()) {
            try self.writeRow();
        }
        try self.select.clearBindings();
    }

    /// writeHeader writes the header to the underlying file.
    fn writeHeader(self: *ExtraTableSaver) !void {
        try self.writer.writeRecord(self.header);
    }

    /// bindSelectArguments binds `self.file_name` as the first parameter of the
    /// SELECT statement.
    fn bindSelectArguments(self: *ExtraTableSaver) !void {
        try self.select.bind(1, self.file_name);
    }

    /// writeRow rewrites a row from SQL to GTFS - should be called only after
    /// `self.select.step` returns true.
    fn writeRow(self: *ExtraTableSaver) !void {
        var fields = OwnedExtraFields.init(self.allocator);
        defer fields.deinit();

        var fields_json: []const u8 = undefined;
        self.select.column(0, &fields_json);
        try fields.parse(fields_json);

        for (self.header) |field_name| {
            const value = fields.get(field_name);
            try self.writer.writeField(value);
        }
        try self.writer.terminateRecord();
    }
};

/// saveTable opens the necessary resources, initializes an `ExtraTableSaver` and calls
/// `ExtraTableSaver.writeAll` to fully rewrite a generic table stored
/// (stored in `extra_table_rows`) to GTFS.
fn saveExtraTable(
    allocator: Allocator,
    gtfs_dir: fs.Dir,
    db_path: [*:0]const u8,
    file_name: [*:0]const u8,
    header: c_char_p_p,
) !void {
    var db = try sqlite3.Connection.init(
        db_path,
        .{ .mode = .read_only, .threading_mode = .no_mutex },
    );
    defer db.deinit();

    var file_buffer: [8192]u8 = undefined;
    var file = try gtfs_dir.createFileZ(file_name, .{});
    defer file.close();
    var writer = file.writer(&file_buffer);

    var saver = try ExtraTableSaver.init(
        db,
        &writer.interface,
        file_name,
        header,
        allocator,
    );
    defer saver.deinit();
    try saver.writeAll();
    try writer.interface.flush();
    std.log.debug("Saving {s} completed", .{file_name});
}

/// saveExtraTableInThread calls `saveExtraTable` to fully rewrite a generic table to GTFS.
///
/// This function is meant to be run in a separate thread - a new `Allocator` is automatically
/// created; `wg.finish` is called on exit, and `failure` is set to true if `saveTable` fails.
fn saveExtraTableInThread(
    gtfs_dir: fs.Dir,
    db_path: [*:0]const u8,
    file_name: [*:0]const u8,
    header: c_char_p_p,
    wg: *Thread.WaitGroup,
    failure: *Atomic(bool),
) void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer std.debug.assert(gpa.deinit() == .ok);
    // const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;
    const allocator = std.heap.c_allocator;

    defer wg.finish();

    saveExtraTable(
        allocator,
        gtfs_dir,
        db_path,
        file_name,
        header,
    ) catch |err| {
        failure.store(true, .release);

        if (@errorReturnTrace()) |trace| {
            std.log.err(
                "gtfs.save: {s}: {t}\nStack trace: {f}",
                .{ file_name, err, trace },
            );
        } else {
            std.log.err("gtfs.save: {s}: {t}", .{ file_name, err });
        }
        return;
    };
}

/// OwnedExtraFields represents a parsed `extra_fields_json` field.
const OwnedExtraFields = struct {
    arena: std.heap.ArenaAllocator,
    map: std.StringArrayHashMapUnmanaged([]const u8) = .{},

    inline fn init(allocator: Allocator) OwnedExtraFields {
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

/// ownedSliceOverCStrings converts a c_char_p_p to a slice of slices.
/// The returned slice must be later deallocated with `allocator.free`.
fn ownedSliceOverCStrings(ptr: c_char_p_p, allocator: Allocator) ![]const [:0]const u8 {
    var slices: std.ArrayList([:0]const u8) = .{};
    defer slices.deinit(allocator);

    var i: usize = 0;
    while (ptr[i] != null) : (i += 1) {
        try slices.append(allocator, std.mem.span(ptr[i].?));
    }

    return try slices.toOwnedSlice(allocator);
}

/// isCalendarEmpty returns true if none of the weekdays of a calendar are set to zero.
///
/// select must be a "SELECT xxx FROM calendars" statement pointing to a valid row,
/// with the 2nd column representing Monday, 3rd - Tuesday, etc. up to 8th - Sunday.
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

    try saveTable(
        std.testing.allocator,
        out_dir.dir,
        "tests/tasks/fixtures/wkd.db",
        &tables[5],
        header,
        false,
        true,
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

    const header_slice = &[_]?c_char_p{ "agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_email", null };
    const header: c_char_p_p = header_slice[0 .. header_slice.len - 1 :null];

    var w = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer w.deinit();

    var s = try TableSaver.init(
        db,
        &w.writer,
        &tables[0],
        header,
        false,
        true,
        std.testing.allocator,
    );
    defer s.deinit();

    try s.writeAll();

    const content = w.written();
    try std.testing.expectEqualStrings(
        // zig fmt: off
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_email\r\n"
        ++ "0,Foo,https://example.com,UTC,en,foo@example.com\r\n"
        ++ "1,Bar,https://example.com,UTC,en,\r\n",
        // zig fmt: on
        content,
    );
}

test "gtfs.save.extra_files" {
    var db = try sqlite3.Connection.init(":memory:", .{});
    defer db.deinit();

    try db.execMany(
        \\ CREATE TABLE extra_table_rows (
        \\    extra_table_row_id INTEGER PRIMARY KEY,
        \\    table_name TEXT NOT NULL,
        \\    fields_json TEXT NOT NULL DEFAULT '{}',
        \\    row_sort_order INTEGER
        \\ ) STRICT;
        \\ CREATE INDEX idx_extra_table_rows_table_row ON extra_table_rows(table_name, row_sort_order);
    );
    try db.exec("BEGIN");
    try db.exec(
        \\ INSERT INTO extra_table_rows (table_name, fields_json, row_sort_order) VALUES
        \\ ('foo.txt', '{"foo":"1","bar":"Hello","baz":"42"}', 0)
    );
    try db.exec(
        \\ INSERT INTO extra_table_rows (table_name, fields_json, row_sort_order) VALUES
        \\ ('foo.txt', '{"foo":"2","bar":"World","baz":""}', 1)
    );
    try db.exec(
        \\ INSERT INTO extra_table_rows (table_name, fields_json, row_sort_order) VALUES
        \\ ('bar.txt', '{"spam":"eggs"}', 0)
    );
    try db.exec("COMMIT");

    const header: []const ?c_char_p = &.{ "foo", "bar", "spam", null };
    const header_null_terminated: [:null]const ?c_char_p = header[0..3 :null];

    var w = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer w.deinit();

    var s = try ExtraTableSaver.init(
        db,
        &w.writer,
        "foo.txt",
        header_null_terminated.ptr,
        std.testing.allocator,
    );
    defer s.deinit();
    try s.writeAll();

    const content = w.written();
    try std.testing.expectEqualStrings(
        "foo,bar,spam\r\n1,Hello,\r\n2,World,\r\n",
        content,
    );
}
