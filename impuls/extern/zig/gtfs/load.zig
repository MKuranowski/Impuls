const c = @import("./conversion.zig");
const csv = @import("../csv.zig");
const logging = @import("../logging.zig");
const std = @import("std");
const sqlite3 = @import("../sqlite3.zig");
const t = @import("./table.zig");

const Allocator = std.mem.Allocator;
const ColumnValue = @import("./conversion.zig").ColumnValue;
const fs = std.fs;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;
const Logger = logging.Logger;
const Table = t.Table;
const tables = t.tables;

pub fn load(logger: Logger, db_path: [*:0]const u8, gtfs_dir_path: [*:0]const u8) !void {
    var db = try sqlite3.Connection.init(db_path, .{});
    defer db.deinit();
    try db.exec("PRAGMA foreign_keys=1");

    var gtfs_dir = try fs.cwd().openDirZ(gtfs_dir_path, .{});
    defer gtfs_dir.close();

    var gpa = GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    inline for (tables) |table| {
        try loadTable(logger, db, gtfs_dir, allocator, table);
    }
}

fn loadTable(
    logger: Logger,
    db: sqlite3.Connection,
    gtfs_dir: fs.Dir,
    allocator: Allocator,
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
    var loader = try Loader.init(logger, db, buffer.reader(), allocator);
    defer loader.deinit();

    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch {};
    try loader.load();
    try db.exec("COMMIT");
}

/// TableLoader loads GTFS data from the provided reader into an SQL table.
fn TableLoader(comptime table: Table, comptime ReaderType: anytype) type {
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

        /// header maps table column indices into GTFS record indices.
        header: [table.columns.len]?usize = .{@as(?usize, null)} ** table.columns.len,

        /// header_len contains the expected number of fields in every GTFS record.
        header_len: usize = 0,

        /// insert is the compiled INSERT INTO ... SQL statement
        insert: sqlite3.Statement,

        /// pi_gtfs_key_column contains the GTFS column index of the implied parent ID.
        pi_gtfs_key_column: if (has_pi) usize else void,

        /// parent_insert is the compiled INSERT OR IGNORE INTO parent_table SQL statement,
        /// which ensures the implied parent exists in the SQL statement.
        parent_insert: if (has_pi) sqlite3.Statement else void,

        /// init creates a TableLoader for a given DB connection and CSV file.
        /// Necessary INSERT statements are compiled.
        fn init(logger: Logger, db: sqlite3.Connection, reader: ReaderType, allocator: Allocator) !Self {
            const csv_reader = csv.reader(reader);
            var csv_record = csv.Record.init(allocator);
            errdefer csv_record.deinit();

            const table_column_names = comptime table.columnNames();
            const table_placeholders = comptime table.placeholders();
            var insert = db.prepare(
                "INSERT INTO " ++ table.sql_name ++ " (" ++ table_column_names ++ ") VALUES (" ++ table_placeholders ++ ")",
            ) catch |err| {
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
                    .insert = insert,
                    .pi_gtfs_key_column = 0,
                    .parent_insert = parent_insert,
                };
            } else {
                return Self{
                    .logger = logger,
                    .reader = csv_reader,
                    .record = csv_record,
                    .insert = insert,
                    .pi_gtfs_key_column = {},
                    .parent_insert = {},
                };
            }
        }

        /// deinit deallocates any resources used by the TableLoader.
        fn deinit(self: *Self) void {
            if (has_pi) self.parent_insert.deinit();
            self.insert.deinit();
            self.record.deinit();
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
        /// Initializes `header_len`, `header` and `pi_gtfs_key_column`.
        ///
        /// Returns false is there is no header row.
        fn loadHeader(self: *Self) !bool {
            if (!try self.reader.next(&self.record)) return false;

            self.header_len = self.record.len();
            var has_pi_gtfs_column = false;

            for (self.record.slice(), 0..) |gtfs_col_name, gtfs_col_idx| {
                if (gtfs_column_name_to_index.get(gtfs_col_name.items)) |table_col_idx| {
                    self.header[table_col_idx] = gtfs_col_idx;
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

            return true;
        }

        /// loadRecord attempts to load `self.record` into the SQL table.
        fn loadRecord(self: Self) !void {
            try self.ensureRecordHasEnoughColumns();
            if (has_pi) try self.loadParentRecord();
            var args = try self.prepareInsertArguments();
            try self.insert.reset();
            try self.bindInsertArguments(&args);
            try self.executeInsert();
            try self.insert.clearBindings();
        }

        /// ensureRecordHasEnoughColumns raises an error if the number of fields inside of the
        /// current record is different than the number of fields inside of the header.
        fn ensureRecordHasEnoughColumns(self: Self) !void {
            if (self.record.len() != self.header_len) {
                self.logger.err(
                    "{s}:{d}: expected {d} columns, got {d}",
                    .{ table.gtfs_name, self.record.line_no, self.header_len, self.record.len() },
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

        /// prepareInsertArguments tries to parse the current GTFS record into
        /// a list of SQL column values.
        ///
        /// Apart from raising an error on invalid GTFS data, a more human-readable, detailed
        /// message is logged.
        ///
        /// Not that, due to lifetime constraints, once the arguments are binded,
        /// they must live until a call to clearBindings. Therefore, it's not possible
        /// to do `for (table.columns) |col| self.insert.bind(col.from_gtfs(...))`.
        fn prepareInsertArguments(self: Self) ![table.columns.len]ColumnValue {
            var arguments: [table.columns.len]ColumnValue = undefined;
            inline for (table.columns, 0..) |col, i| {
                const raw_value: []const u8 = if (self.header[i]) |j| self.record.get(j) else "";
                arguments[i] = col.from_gtfs(raw_value, self.record.line_no) catch |err| {
                    self.logger.err(
                        "{s}:{d}:{s}: {}",
                        .{ table.gtfs_name, self.record.line_no, col.gtfsName(), err },
                    );
                    return err;
                };
            }
            return arguments;
        }

        /// bindInsertArguments tries to bind the provided SQL values to the insert statement.
        ///
        /// Note that the arguments must be passed by reference - in order to prevent ColumnValues
        /// being moved around in memory, to satisfy sqlite3's lifetime requirements for strings.
        fn bindInsertArguments(self: Self, args: *const [table.columns.len]ColumnValue) !void {
            for (args, 1..) |*arg, i| {
                try arg.bind(self.insert, @intCast(i));
            }
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
    var loader = try Loader.init(logging.StderrLogger, db, reader, std.testing.allocator);
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
    var loader = try Loader.init(logging.StderrLogger, db, reader, std.testing.allocator);
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
