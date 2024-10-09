// © Copyright 2022-2024 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const c = @import("./conversion.zig");
const csv = @import("../csv.zig");
const logging = @import("../logging.zig");
const std = @import("std");
const sqlite3 = @import("../sqlite3.zig");
const t = @import("./table.zig");

const Allocator = std.mem.Allocator;
const ColumnMapping = t.ColumnMapping;
const ColumnValue = @import("./conversion.zig").ColumnValue;
const fs = std.fs;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;
const Logger = logging.Logger;
const Table = t.Table;
const tables = t.tables;

pub fn load(
    logger: Logger,
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    extra_fields: bool,
) !void {
    var db = try sqlite3.Connection.init(db_path, .{});
    defer db.deinit();
    try db.exec("PRAGMA foreign_keys=1");

    var gtfs_dir = try fs.cwd().openDirZ(gtfs_dir_path, .{});
    defer gtfs_dir.close();

    var gpa = GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    inline for (tables) |table| {
        try loadTable(
            logger,
            db,
            gtfs_dir,
            allocator,
            extra_fields,
            table,
        );
    }
}

fn loadTable(
    logger: Logger,
    db: sqlite3.Connection,
    gtfs_dir: fs.Dir,
    allocator: Allocator,
    extra_fields: bool,
    comptime table: Table,
) !void {
    var file = gtfs_dir.openFileZ(table.gtfs_name, .{}) catch |err| {
        if (err == error.FileNotFound) {
            if (table.required) {
                logger.err("Missing required table " ++ table.gtfs_name, .{});
                return err;
            } else {
                return {};
            }
        }
        return err;
    };
    var buffer = std.io.bufferedReaderSize(8192, file.reader());
    logger.debug("Loading " ++ table.gtfs_name, .{});

    const Loader = comptime TableLoader(table, @TypeOf(buffer).Reader);
    var loader = try Loader.init(
        logger,
        db,
        buffer.reader(),
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
fn TableLoader(comptime table: Table, comptime ReaderType: anytype) type {
    const ColumnBuffer = std.BoundedArray(ColumnMapping, 32);
    const has_pi = comptime table.parent_implication != null;
    const gtfs_column_name_to_index = comptime table.gtfsColumnNamesToIndices();

    return struct {
        const Self = @This();

        /// logger is used to report on any issues and progress on table loading.
        logger: Logger,

        /// reader reads data from the provided GTFargs: ...S file.
        reader: csv.Reader(ReaderType),

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
            logger: Logger,
            db: sqlite3.Connection,
            reader: ReaderType,
            allocator: Allocator,
            load_extra_fields: bool,
        ) !Self {
            const csv_reader = csv.reader(reader);
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
                logger.err(
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
                    logger.err(
                        "{s}: failed to compile INSERT OR IGNORE INTO: {s}",
                        .{ table.gtfs_name, db.errMsg() },
                    );
                    return err;
                };

                return Self{
                    .logger = logger,
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
                    .logger = logger,
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
                self.logger.err(
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
                self.logger.err(
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
                self.logger.err(
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

            for (self.header.slice(), 0..) |column_mapping, gtfs_idx| {
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
                    self.logger.err(
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
                self.logger.err(
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
                self.extra_serialized = try std.json.stringifyAlloc(
                    self.arena.?.allocator(),
                    self.extra,
                    .{ .escape_unicode = true },
                );
            } else {
                self.extra_serialized = null;
            }
        }
    };
}

test "gtfs.load.simple" {
    const from_gtfs = @import("./conversion_from_gtfs.zig");

    var db = try sqlite3.Connection.init(":memory:", .{});
    defer db.deinit();

    try db.exec("CREATE TABLE spam (foo TEXT PRIMARY KEY, bar INTEGER NOT NULL, baz TEXT NOT NULL DEFAULT '') STRICT");

    const data = "foo,baz,bar\r\n1,Hello,42\r\n2,World,\r\n";
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    const table = comptime Table{
        .gtfs_name = "spam.txt",
        .sql_name = "spam",
        .columns = &[_]t.Column{
            t.Column{ .name = "foo" },
            t.Column{ .name = "bar", .from_gtfs = from_gtfs.intFallbackZero },
            t.Column{ .name = "baz" },
        },
    };

    const Loader = TableLoader(table, @TypeOf(reader));
    var loader = try Loader.init(
        logging.StderrLogger,
        db,
        reader,
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
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

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

    const Loader = TableLoader(table, @TypeOf(reader));
    var loader = try Loader.init(logging.StderrLogger, db, reader, std.testing.allocator, false);
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
