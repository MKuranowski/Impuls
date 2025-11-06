// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const builtin = @import("builtin");
const c = @import("./conversion.zig");
const csv = @import("../csv.zig");
const std = @import("std");
const sqlite3 = @import("../sqlite3.zig");
const t = @import("./table.zig");

const Allocator = std.mem.Allocator;
const BoundedArray = @import("../bounded_array.zig").BoundedArray;
const ColumnMapping = t.ColumnMapping;
const ColumnValue = @import("./conversion.zig").ColumnValue;
const fs = std.fs;
const Table = t.Table;
const tables = t.tables;

pub fn load(
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    extra_fields: bool,
    extra_files: []const [*:0]const u8,
) !void {
    var db = try sqlite3.Connection.init(db_path, .{});
    defer db.deinit();
    try db.exec("PRAGMA foreign_keys=1");

    var gtfs_dir = try fs.cwd().openDirZ(gtfs_dir_path, .{});
    defer gtfs_dir.close();

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer std.debug.assert(gpa.deinit() == .ok);
    // const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;
    const allocator = std.heap.c_allocator;

    inline for (tables) |table| {
        try loadTable(
            db,
            gtfs_dir,
            allocator,
            extra_fields,
            table,
        );
    }

    for (extra_files) |extra_file| {
        try loadExtraTable(
            db,
            gtfs_dir,
            allocator,
            extra_file,
        );
    }
}

fn loadTable(
    db: sqlite3.Connection,
    gtfs_dir: fs.Dir,
    allocator: Allocator,
    extra_fields: bool,
    comptime table: Table,
) !void {
    var file_buffer: [8192]u8 = undefined;
    var file = gtfs_dir.openFileZ(table.gtfs_name, .{}) catch |err| {
        if (err == error.FileNotFound) {
            if (table.required) {
                std.log.err("Missing required table " ++ table.gtfs_name, .{});
                return err;
            } else {
                return {};
            }
        }
        return err;
    };
    defer file.close();
    var reader = file.reader(&file_buffer);
    std.log.debug("Loading " ++ table.gtfs_name, .{});

    const Loader = comptime TableLoader(table);
    var loader = try Loader.init(
        db,
        &reader.interface,
        allocator,
        extra_fields,
    );
    defer loader.deinit();

    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch {};
    try loader.load();
    try db.exec("COMMIT");
}

/// TableLoader loads GTFS data from the provided reader into an SQL table.
fn TableLoader(comptime table: Table) type {
    const ColumnBuffer = BoundedArray(ColumnMapping, 32);
    const has_pi = comptime table.parent_implication != null;
    const gtfs_column_name_to_index = comptime table.gtfsColumnNamesToIndices();

    return struct {
        const Self = @This();

        /// reader reads data from the provided GTFS file.
        reader: csv.Reader,

        /// record stores the recently-read row from the GTFS file.
        record: csv.Record,

        /// record stores the header row from the GTFS file.
        header_record: csv.Record,

        /// header maps table column indices into GTFS record indices.
        header: ColumnBuffer = .{},

        /// insert is the compiled INSERT INTO ... SQL statement
        insert: sqlite3.Statement,

        /// has_extra_fields is set to true if there's an extra parameter in the INSERT
        /// statement corresponding to the `extra_fields_json`. That parameter is always
        /// after all of the `table.columns` parameters, that is its one-based index is
        /// `table.columns.len + 1`.
        has_extra_fields: bool = false,

        /// pi_gtfs_key_column contains the GTFS column index of the implied parent ID.
        pi_gtfs_key_column: if (has_pi) usize else void,

        /// parent_insert is the compiled INSERT OR IGNORE INTO parent_table SQL statement,
        /// which ensures the implied parent exists in the SQL statement.
        parent_insert: if (has_pi) sqlite3.Statement else void,

        /// init creates a TableLoader for a given DB connection and CSV file.
        /// Necessary INSERT statements are compiled.
        fn init(
            db: sqlite3.Connection,
            reader: *std.Io.Reader,
            allocator: Allocator,
            load_extra_fields: bool,
        ) !Self {
            const csv_reader = csv.Reader.init(reader);
            var csv_record = csv.Record.init(allocator);
            errdefer csv_record.deinit();
            var csv_header_record = csv.Record.init(allocator);
            errdefer csv_header_record.deinit();

            const column_names = comptime table.columnNames();
            const placeholders = comptime table.placeholders();
            const has_extra_fields = load_extra_fields and table.has_extra_fields_json;
            const insert_sql = if (has_extra_fields)
                comptime "INSERT INTO " ++ table.sql_name ++ " (" ++ column_names ++ ", extra_fields_json) VALUES (" ++ placeholders ++ ", ?)"
            else
                comptime "INSERT INTO " ++ table.sql_name ++ " (" ++ column_names ++ ") VALUES (" ++ placeholders ++ ")";
            var insert = db.prepare(insert_sql) catch |err| {
                std.log.err(
                    "{s}: failed to compile INSERT INTO: {s}",
                    .{ table.gtfs_name, db.errMsg() },
                );
                return err;
            };
            errdefer insert.deinit();

            if (has_pi) {
                const parent_insert = db.prepare(
                    "INSERT OR IGNORE INTO " ++ table.parent_implication.?.sql_table ++ " (" ++ table.parent_implication.?.sql_key ++ ") VALUES (?)",
                ) catch |err| {
                    std.log.err(
                        "{s}: failed to compile INSERT OR IGNORE INTO: {s}",
                        .{ table.gtfs_name, db.errMsg() },
                    );
                    return err;
                };

                return Self{
                    .reader = csv_reader,
                    .record = csv_record,
                    .header_record = csv_header_record,
                    .insert = insert,
                    .pi_gtfs_key_column = 0,
                    .parent_insert = parent_insert,
                    .has_extra_fields = has_extra_fields,
                };
            } else {
                return Self{
                    .reader = csv_reader,
                    .record = csv_record,
                    .header_record = csv_header_record,
                    .insert = insert,
                    .pi_gtfs_key_column = {},
                    .parent_insert = {},
                    .has_extra_fields = has_extra_fields,
                };
            }
        }

        /// deinit deallocates any resources used by the TableLoader.
        fn deinit(self: *Self) void {
            if (has_pi) self.parent_insert.deinit();
            self.insert.deinit();
            self.record.deinit();
            self.header_record.deinit();
        }

        /// load actually loads data from the GTFS table to the SQL table.
        ///
        /// Only the necessary INSERT statements are executed - for performance reasons the caller
        /// is advised to initiate a transaction.
        fn load(self: *Self) !void {
            if (!try self.loadHeader()) return;
            while (try self.reader.next(&self.record)) {
                try self.loadRecord();
            }
        }

        /// loadHeader loads a record from the GTFS table and processes it as the header row.
        /// Initializes `header_record`, `header` and `pi_gtfs_key_column`.
        ///
        /// Returns false is there is no header row.
        fn loadHeader(self: *Self) !bool {
            if (!try self.reader.next(&self.header_record)) return false;
            var has_pi_gtfs_column = false;

            for (self.header_record.slice(), 0..) |gtfs_col_name, gtfs_col_idx| {
                if (gtfs_column_name_to_index.get(gtfs_col_name.items)) |table_col_idx| {
                    try self.header.append(.{ .standard = table_col_idx });
                } else if (self.has_extra_fields) {
                    try self.header.append(.{ .extra = gtfs_col_name.items });
                } else {
                    try self.header.append(.{ .none = {} });
                }

                if (has_pi and std.mem.eql(u8, gtfs_col_name.items, table.parent_implication.?.gtfs_key)) {
                    self.pi_gtfs_key_column = gtfs_col_idx;
                    has_pi_gtfs_column = true;
                }
            }

            if (has_pi and !has_pi_gtfs_column) {
                std.log.err(
                    "{s}:{d}: missing required column: {s}",
                    .{ table.gtfs_name, self.record.line_no, table.parent_implication.?.gtfs_key },
                );
                return error.MissingRequiredColumn;
            }

            self.record.line_no = self.header_record.line_no;
            return true;
        }

        /// loadRecord attempts to load `self.record` into the SQL table.
        fn loadRecord(self: Self) !void {
            try self.ensureRecordHasEnoughColumns();
            if (has_pi) try self.loadParentRecord();
            var args = try self.prepareInsertArguments();
            defer args.deinit();
            try self.insert.reset();
            try args.bind(self.insert, self.has_extra_fields);
            try self.executeInsert();
            try self.insert.clearBindings();
        }

        /// ensureRecordHasEnoughColumns raises an error if the number of fields inside of the
        /// current record is different than the number of fields inside of the header.
        fn ensureRecordHasEnoughColumns(self: Self) !void {
            if (self.record.len() != self.header_record.len()) {
                std.log.err(
                    "{s}:{d}: expected {d} columns, got {d}",
                    .{ table.gtfs_name, self.record.line_no, self.header_record.len(), self.record.len() },
                );
                return error.MisalignedCSV;
            }
        }

        /// loadParentRecord ensures the implied parent entity exists.
        /// `has_pi` must be true in order to call this method.
        fn loadParentRecord(self: Self) !void {
            const key = self.record.get(self.pi_gtfs_key_column);

            try self.parent_insert.reset();
            try self.parent_insert.bind(1, key);
            self.parent_insert.stepUntilDone() catch |err| {
                std.log.err(
                    "{s}:{d}: {}: {s}",
                    .{ table.gtfs_name, self.record.line_no, err, self.parent_insert.errMsg() },
                );
                return err;
            };

            try self.parent_insert.clearBindings();
        }

        /// prepareInsertArguments tries to parse the current GTFS record into `InsertArguments`.
        ///
        /// Apart from raising an error on invalid GTFS data, a more human-readable, detailed
        /// message is logged.
        ///
        /// Note that, due to lifetime constraints, the returned InsertArguments can't be moved
        /// in memory.
        fn prepareInsertArguments(self: Self) !InsertArguments(table.columns.len) {
            var arguments: InsertArguments(table.columns.len) = .{};
            errdefer arguments.deinit();
            if (self.has_extra_fields) {
                arguments.arena = std.heap.ArenaAllocator.init(self.record.allocator);
            }

            var sql_to_gtfs_idx = [_]?usize{null} ** table.columns.len;

            for (self.header.constSlice(), 0..) |column_mapping, gtfs_idx| {
                const raw_value = self.record.get(gtfs_idx);
                switch (column_mapping) {
                    .standard => |sql_idx| {
                        sql_to_gtfs_idx[sql_idx] = gtfs_idx;
                    },
                    .extra => |extra_column_name| {
                        try arguments.set_extra_arg(extra_column_name, raw_value);
                    },
                    .none => {},
                }
            }

            for (table.columns, 0..) |col, sql_idx| {
                const gtfs_idx = sql_to_gtfs_idx[sql_idx];
                const gtfs_value: []const u8 = if (gtfs_idx) |i| self.record.get(i) else "";
                arguments.standard[sql_idx] = col.from_gtfs(gtfs_value, self.record.line_no) catch |err| {
                    std.log.err(
                        "{s}:{d}:{s}: {}",
                        .{ table.gtfs_name, self.record.line_no, col.gtfsName(), err },
                    );
                    return err;
                };
            }

            return arguments;
        }

        /// executeInsert tries to execute the SQL INSERT statement. Apart from raising an error
        /// on any issues, a more human-readable, detailed message is logged.
        fn executeInsert(self: Self) !void {
            self.insert.stepUntilDone() catch |err| {
                std.log.err(
                    "{s}:{d}: {}: {s}",
                    .{ table.gtfs_name, self.record.line_no, err, self.insert.errMsg() },
                );
                return err;
            };
        }
    };
}

/// InsertArguments represents arguments to an INSERT statement.
fn InsertArguments(comptime table_columns: usize) type {
    return struct {
        const Self = @This();

        /// arena is used to allocate space for `extra` and `extra_serialized`.
        /// May be left as `null` if those fields are not used.
        arena: ?std.heap.ArenaAllocator = null,

        /// standard represents values to be bound to the first `table_columns`.
        /// By default, all vaules are set to Null.
        standard: [table_columns]ColumnValue = [_]ColumnValue{.Null} ** table_columns,

        /// extra is a storage container for extra fields, to be used
        /// for an optional last argument. The map is allocated using `arena`.
        extra: std.json.ArrayHashMap([]const u8) = .{},

        /// extra_serialized contains a JSON representation of `extra`.
        /// Allocated using `arena` automatically in `bind` and `bindExtraArguments`.
        extra_serialized: ?[]const u8 = null,

        fn deinit(self: *Self) void {
            if (self.arena) |a| a.deinit();
        }

        fn set_extra_arg(self: *Self, k: []const u8, v: []const u8) !void {
            try self.extra.map.put(self.arena.?.allocator(), k, v);
        }

        fn bind(self: *Self, stmt: sqlite3.Statement, has_extra_fields: bool) !void {
            try self.bindStandardArguments(stmt);
            if (has_extra_fields) try self.bindExtraArguments(stmt);
        }

        fn bindStandardArguments(self: *Self, stmt: sqlite3.Statement) !void {
            for (&self.standard, 1..) |*column_value, i| {
                try column_value.bind(stmt, @intCast(i));
            }
        }

        fn bindExtraArguments(self: *Self, stmt: sqlite3.Statement) !void {
            try self.ensureExtraSerialized();
            try stmt.bind(@intCast(table_columns + 1), self.extra_serialized);
        }

        fn ensureExtraSerialized(self: *Self) !void {
            if (self.extra.map.count() > 0) {
                var w = std.Io.Writer.Allocating.init(self.arena.?.allocator());
                defer w.deinit();
                try std.json.Stringify.value(self.extra, .{ .escape_unicode = true }, &w.writer);
                self.extra_serialized = try w.toOwnedSlice();
            } else {
                self.extra_serialized = null;
            }
        }
    };
}

fn loadExtraTable(
    db: sqlite3.Connection,
    gtfs_dir: fs.Dir,
    allocator: Allocator,
    extra_file: [*:0]const u8,
) !void {
    var file_buffer: [8192]u8 = undefined;
    var file = gtfs_dir.openFileZ(extra_file, .{}) catch |err| {
        if (err == error.FileNotFound) {
            std.log.warn("Missing extra file: {s}", .{extra_file});
            return;
        }
        return err;
    };
    defer file.close();
    var reader = file.reader(&file_buffer);
    std.log.debug("Loading {s}", .{extra_file});

    var loader = try ExtraTableLoader.init(
        db,
        &reader.interface,
        std.mem.span(extra_file),
        allocator,
    );
    defer loader.deinit();

    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch {};
    try loader.load();
    try db.exec("COMMIT");
}

const ExtraTableLoader = struct {
    /// reader reads data from the provided GTFS file.
    reader: csv.Reader,

    /// record stores the recently-read row from the GTFS file.
    record: csv.Record,

    /// record_map stores a mapping from header fields to record fields,
    /// for serialization into the "fields_json" SQL field.
    record_map: std.json.ArrayHashMap([]const u8) = .{},

    /// record_map_json stores the serialization of `record_map`.
    record_map_json: std.Io.Writer.Allocating,

    /// header stores the header row from the GTFS file.
    header: csv.Record,

    /// insert is the compiled INSERT INTO ... SQL statement
    insert: sqlite3.Statement,

    /// table_name is the full name of the loaded table
    table_name: [:0]const u8,

    /// init creates an ExtraTableLoader for a given DB connection and CSV file.
    /// The necessary INSERT statement is compiled.
    inline fn init(
        db: sqlite3.Connection,
        reader: *std.Io.Reader,
        table_name: [:0]const u8,
        allocator: Allocator,
    ) !ExtraTableLoader {
        return .{
            .reader = csv.Reader.init(reader),
            .record = csv.Record.init(allocator),
            .record_map_json = std.Io.Writer.Allocating.init(allocator),
            .header = csv.Record.init(allocator),
            .insert = try db.prepare(
                \\ INSERT INTO extra_table_rows
                \\ (table_name, fields_json, row_sort_order)
                \\ VALUES (?, ?, ?)
            ),
            .table_name = table_name,
        };
    }

    /// getAllocator returns an Allocator available for this ExtraTableLoader.
    inline fn getAllocator(self: ExtraTableLoader) Allocator {
        return self.record.allocator;
    }

    /// deinit deallocates any resources used by the ExtraTableLoader.
    fn deinit(self: *ExtraTableLoader) void {
        self.record_map.deinit(self.getAllocator());
        self.record_map_json.deinit();
        self.record.deinit();
        self.header.deinit();
        self.insert.deinit();
    }

    fn load(self: *ExtraTableLoader) !void {
        if (!try self.loadHeader()) return;
        var sort_order: usize = 0;
        while (try self.reader.next(&self.record)) : (sort_order += 1) {
            try self.loadRecord(sort_order);
        }
    }

    /// loadHeader loads a record from the GTFS table and processes it as the header row.
    /// Initializes `header`.
    ///
    /// Returns false is there is no header row.
    fn loadHeader(self: *ExtraTableLoader) !bool {
        const exists = self.reader.next(&self.header);
        self.record.line_no = self.header.line_no;
        return exists;
    }

    /// loadRecord attempts to load `self.record` into the SQL table.
    fn loadRecord(self: *ExtraTableLoader, sort_order: usize) !void {
        try self.ensureRecordHasEnoughColumns();
        try self.loadRecordMap();
        try self.serializeRecordMap();
        try self.insert.reset();
        try self.bindRecordArguements(sort_order);
        try self.insert.stepUntilDone();
        try self.insert.clearBindings();
    }

    /// ensureRecordHasEnoughColumns raises an error if the number of fields inside of the
    /// current record is different than the number of fields inside of the header.
    fn ensureRecordHasEnoughColumns(self: ExtraTableLoader) !void {
        if (self.record.len() != self.header.len()) {
            std.log.err(
                "{s}:{d}: expected {d} columns, got {d}",
                .{ self.table_name, self.record.line_no, self.header.len(), self.record.len() },
            );
            return error.MisalignedCSV;
        }
    }

    /// loadRecordMap re-initializes `record_map` values.
    fn loadRecordMap(self: *ExtraTableLoader) !void {
        for (0..self.header.len()) |i| {
            const key = self.header.get(i);
            const value = self.record.get(i);
            try self.record_map.map.put(self.getAllocator(), key, value);
        }
    }

    /// serializeRecordMap re-initializes `record_map_json` string.
    fn serializeRecordMap(self: *ExtraTableLoader) !void {
        self.record_map_json.clearRetainingCapacity();
        try std.json.Stringify.value(self.record_map, .{ .escape_unicode = true }, &self.record_map_json.writer);
    }

    /// bindRecordArguements binds the 2nd and 3rd arguments of the INSERT statement.
    fn bindRecordArguements(self: *ExtraTableLoader, sort_order: usize) !void {
        try self.insert.bind(1, self.table_name);
        try self.insert.bind(2, self.record_map_json.written());
        try self.insert.bind(3, sort_order);
    }
};

test "gtfs.load.simple" {
    const from_gtfs = @import("./conversion_from_gtfs.zig");

    var db = try sqlite3.Connection.init(":memory:", .{});
    defer db.deinit();

    try db.exec("CREATE TABLE spam (foo TEXT PRIMARY KEY, bar INTEGER NOT NULL, baz TEXT NOT NULL DEFAULT '') STRICT");

    const data = "foo,baz,bar\r\n1,Hello,42\r\n2,World,\r\n";
    var reader = std.Io.Reader.fixed(data);

    const table = comptime Table{
        .gtfs_name = "spam.txt",
        .sql_name = "spam",
        .columns = &[_]t.Column{
            t.Column{ .name = "foo" },
            t.Column{ .name = "bar", .from_gtfs = from_gtfs.intFallbackZero },
            t.Column{ .name = "baz" },
        },
    };

    const Loader = TableLoader(table);
    var loader = try Loader.init(
        db,
        &reader,
        std.testing.allocator,
        false,
    );
    defer loader.deinit();

    try db.exec("BEGIN");
    try loader.load();
    try db.exec("COMMIT");

    var select = try db.prepare("SELECT * FROM spam ORDER BY foo ASC");
    defer select.deinit();

    var s: []const u8 = undefined;
    var i: i64 = undefined;

    try std.testing.expect(try select.step());
    try std.testing.expectEqual(@as(c_int, 3), select.columnCount());
    select.column(0, &s);
    try std.testing.expectEqualStrings("1", s);
    select.column(1, &i);
    try std.testing.expectEqual(@as(i64, 42), i);
    select.column(2, &s);
    try std.testing.expectEqualStrings("Hello", s);

    try std.testing.expect(try select.step());
    try std.testing.expectEqual(@as(c_int, 3), select.columnCount());
    select.column(0, &s);
    try std.testing.expectEqualStrings("2", s);
    select.column(1, &i);
    try std.testing.expectEqual(@as(i64, 0), i);
    select.column(2, &s);
    try std.testing.expectEqualStrings("World", s);

    try std.testing.expect(!try select.step());
}

test "gtfs.load.with_parent_implication" {
    const from_gtfs = @import("./conversion_from_gtfs.zig");

    var db = try sqlite3.Connection.init(":memory:", .{});
    defer db.deinit();

    try db.exec("PRAGMA foreign_keys=1");
    try db.exec("CREATE TABLE parents (p_id TEXT PRIMARY KEY, attr INTEGER NOT NULL DEFAULT 0) STRICT");
    try db.exec(
        \\ CREATE TABLE children (
        \\  p_id TEXT NOT NULL REFERENCES parents(p_id),
        \\  seq INTEGER NOT NULL CHECK (seq >= 0),
        \\  PRIMARY KEY (p_id, seq)
        \\ ) STRICT
    );

    const data = "parent_id,seq\r\nA,0\r\nA,1\r\nB,1\r\nB,2\r\n";
    var reader = std.Io.Reader.fixed(data);

    const table = comptime Table{
        .gtfs_name = "children.txt",
        .sql_name = "children",
        .parent_implication = t.ParentImplication{
            .sql_table = "parents",
            .sql_key = "p_id",
            .gtfs_key = "parent_id",
        },
        .columns = &[_]t.Column{
            t.Column{ .name = "p_id", .gtfs_name = "parent_id" },
            t.Column{ .name = "seq", .from_gtfs = from_gtfs.int },
        },
    };

    const Loader = TableLoader(table);
    var loader = try Loader.init(db, &reader, std.testing.allocator, false);
    defer loader.deinit();

    try db.exec("BEGIN");
    try loader.load();
    try db.exec("COMMIT");

    var s: []const u8 = undefined;
    var i: i64 = undefined;

    // Ensure that parents were created

    var select_p = try db.prepare("SELECT * FROM parents ORDER BY p_id ASC");
    defer select_p.deinit();

    try std.testing.expect(try select_p.step());
    try std.testing.expectEqual(@as(c_int, 2), select_p.columnCount());
    select_p.column(0, &s);
    try std.testing.expectEqualStrings("A", s);
    select_p.column(1, &i);
    try std.testing.expectEqual(@as(i64, 0), i);

    try std.testing.expect(try select_p.step());
    try std.testing.expectEqual(@as(c_int, 2), select_p.columnCount());
    select_p.column(0, &s);
    try std.testing.expectEqualStrings("B", s);
    select_p.column(1, &i);
    try std.testing.expectEqual(@as(i64, 0), i);

    try std.testing.expect(!try select_p.step());

    // Ensure that children were created

    var select_c = try db.prepare("SELECT * FROM children ORDER BY p_id, seq ASC");
    defer select_c.deinit();

    try std.testing.expect(try select_c.step());
    try std.testing.expectEqual(@as(c_int, 2), select_c.columnCount());
    select_c.column(0, &s);
    try std.testing.expectEqualStrings("A", s);
    select_c.column(1, &i);
    try std.testing.expectEqual(@as(i64, 0), i);

    try std.testing.expect(try select_c.step());
    try std.testing.expectEqual(@as(c_int, 2), select_c.columnCount());
    select_c.column(0, &s);
    try std.testing.expectEqualStrings("A", s);
    select_c.column(1, &i);
    try std.testing.expectEqual(@as(i64, 1), i);

    try std.testing.expect(try select_c.step());
    try std.testing.expectEqual(@as(c_int, 2), select_c.columnCount());
    select_c.column(0, &s);
    try std.testing.expectEqualStrings("B", s);
    select_c.column(1, &i);
    try std.testing.expectEqual(@as(i64, 1), i);

    try std.testing.expect(try select_c.step());
    try std.testing.expectEqual(@as(c_int, 2), select_c.columnCount());
    select_c.column(0, &s);
    try std.testing.expectEqualStrings("B", s);
    select_c.column(1, &i);
    try std.testing.expectEqual(@as(i64, 2), i);

    try std.testing.expect(!try select_c.step());
}

test "gtfs.load.extra" {
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

    const data = "foo,baz,bar\r\n1,Hello,42\r\n2,World,\r\n";
    var reader = std.Io.Reader.fixed(data);

    var loader = try ExtraTableLoader.init(
        db,
        &reader,
        "foo.txt",
        std.testing.allocator,
    );
    defer loader.deinit();

    try db.exec("BEGIN");
    try loader.load();
    try db.exec("COMMIT");

    var select = try db.prepare(
        \\ SELECT table_name, fields_json, row_sort_order
        \\ FROM extra_table_rows
        \\ ORDER BY row_sort_order ASC
    );
    defer select.deinit();

    var table_name: []const u8 = undefined;
    var fields_json: []const u8 = undefined;
    var row_sort_order: usize = undefined;

    try std.testing.expect(try select.step());
    select.columns(.{ &table_name, &fields_json, &row_sort_order });
    try std.testing.expectEqualStrings("foo.txt", table_name);
    try std.testing.expectEqualStrings("{\"foo\":\"1\",\"baz\":\"Hello\",\"bar\":\"42\"}", fields_json);
    try std.testing.expectEqual(@as(usize, 0), row_sort_order);

    try std.testing.expect(try select.step());
    select.columns(.{ &table_name, &fields_json, &row_sort_order });
    try std.testing.expectEqualStrings("foo.txt", table_name);
    try std.testing.expectEqualStrings("{\"foo\":\"2\",\"baz\":\"World\",\"bar\":\"\"}", fields_json);
    try std.testing.expectEqual(@as(usize, 1), row_sort_order);

    try std.testing.expect(!try select.step());
}
