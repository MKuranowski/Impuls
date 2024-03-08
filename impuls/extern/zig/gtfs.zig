const csv = @import("./csv.zig");
const std = @import("std");
const sqlite3 = @import("./sqlite3.zig");
const fmt = std.fmt;
const fs = std.fs;
const Allocator = std.mem.Allocator;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;
const bufferedReaderSize = std.io.bufferedReaderSize;
const isDigit = std.ascii.isDigit;
const assert = std.debug.assert;
const panic = std.debug.panic;
const print = std.debug.print;

comptime {
    // C guarantees that sizeof(char) is 1 byte, but doesn't guarante that one byte is exactly
    // 8 bits. Platforms with non-8-bit-bytes exist, but are extremely uncommon. As this module
    // treats c_char and u8 interchangebly, crash if those types have different sizes.
    if (@typeInfo(c_char).Int.bits != @typeInfo(u8).Int.bits)
        @compileError("u8 and c_char have different widths. This module expectes those types to be interchangable.");
}

/// Non-null pointer to a null-terminated C byte string, aka `char const*`.
const c_char_p = [*:0]const u8;

/// Non-null pointer to a null-terminated vector of c_strings, aka `char const* const*`.
const c_char_p_p = [*:null]const ?c_char_p;

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
};

pub fn load(db_path: [*:0]const u8, gtfs_dir_path: [*:0]const u8) !void {
    var db = try sqlite3.Connection.init(db_path, .{});
    defer db.deinit();

    var gtfs_dir = try fs.cwd().openDirZ(gtfs_dir_path, .{}, false);
    defer gtfs_dir.close();

    var gpa = GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    inline for (tables) |table| {
        try loadTable(db, gtfs_dir, allocator, table);
    }
}

fn loadTable(
    db: sqlite3.Connection,
    gtfs_dir: fs.Dir,
    allocator: Allocator,
    comptime table: Table,
) !void {
    var file = gtfs_dir.openFileZ(table.gtfs_name, .{}) catch |err| {
        if (err == error.FileNotFound) return {};
        return err;
    };
    var buffer = bufferedReaderSize(8192, file.reader());
    print("Loading {s}\n", .{table.gtfs_name});

    const Loader = comptime TableLoader(table, @TypeOf(buffer).Reader);
    var loader = try Loader.init(db, buffer.reader(), allocator);
    defer loader.deinit();

    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch {};
    try loader.load();
    try db.exec("COMMIT");
}

pub fn save(
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    headers: *Headers,
    emit_empty_calendars: bool,
) !void {
    _ = db_path;
    _ = gtfs_dir_path;
    _ = headers;
    _ = emit_empty_calendars;

    return error.NotImplemented;
}

/// TableLoader loads GTFS data from the provided reader into an SQL table.
fn TableLoader(comptime table: Table, comptime ReaderType: anytype) type {
    const has_pi = comptime table.parent_implication != null;
    const gtfs_column_name_to_index = comptime table.gtfsColumnNamesToIndices();

    return struct {
        const Self = @This();

        /// reader reads data from the provided GTFS file.
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
        fn init(db: sqlite3.Connection, reader: ReaderType, allocator: Allocator) !Self {
            var csv_reader = csv.reader(reader);
            var csv_record = csv.Record.init(allocator);
            errdefer csv_record.deinit();

            const table_column_names = comptime table.columnNames();
            const table_placeholders = comptime table.placeholders();
            var insert = db.prepare(
                "INSERT INTO " ++ table.sql_name ++ " " ++ table_column_names ++ " VALUES " ++ table_placeholders,
            ) catch |err| {
                print("{s}: failed to compile INSERT INTO: {s}\n", .{ table.gtfs_name, db.errMsg() });
                return err;
            };
            errdefer insert.deinit();

            if (has_pi) {
                var parent_insert = db.prepare(
                    "INSERT OR IGNORE INTO " ++ table.parent_implication.?.sql_table ++ " (" ++ table.parent_implication.?.sql_key ++ ") VALUES (?)",
                ) catch |err| {
                    print("{s}: failed to compile INSERT OR IGNORE INTO: {s}\n", .{ table.gtfs_name, db.errMsg() });
                    return err;
                };

                return Self{
                    .reader = csv_reader,
                    .record = csv_record,
                    .insert = insert,
                    .pi_gtfs_key_column = 0,
                    .parent_insert = parent_insert,
                };
            } else {
                return Self{
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
                print(
                    "{s}:{d}: missing required column: {s}\n",
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
                print(
                    "{s}:{d}: expected {d} columns, got {d}\n",
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
                print(
                    "{s}:{d}: {}: {s}\n",
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
        /// message is printed to stderr.
        ///
        /// Not that, due to lifetime constraints, once the arguments are binded,
        /// they must live until a call to clearBindings. Therefore, it's not possible
        /// to do `for (table.columns) |col| self.insert.bind(col.from_gtfs(...))`.
        fn prepareInsertArguments(self: Self) ![table.columns.len]ColumnValue {
            var arguments: [table.columns.len]ColumnValue = undefined;
            inline for (table.columns, 0..) |col, i| {
                const raw_value: []const u8 = if (self.header[i]) |j| self.record.get(j) else "";
                arguments[i] = col.from_gtfs(raw_value, self.record.line_no) catch |err| {
                    print(
                        "{s}:{d}:{s}: {}\n",
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
        /// on any issues, a more human-readable, detailed message is printed to stderr.
        fn executeInsert(self: Self) !void {
            self.insert.stepUntilDone() catch |err| {
                print(
                    "{s}:{d}: {}: {s}\n",
                    .{ table.gtfs_name, self.record.line_no, err, self.insert.errMsg() },
                );
                return err;
            };
        }
    };
}

/// Table contains data necessary for mapping between GTFS and Impuls (SQL) tables.
const Table = struct {
    /// gtfs_name constains the GTFS table name, with the .txt extension
    gtfs_name: [:0]const u8,

    /// sql_name contains the Impuls (SQL) table name
    sql_name: [:0]const u8,

    /// columns constains specifics on column mapping between the schemas
    columns: []const Column,

    /// parent_implication, if present, describes the existance of parent objects in GTFS
    parent_implication: ?ParentImplication = null,

    /// columnNames returns a "(column_a, column_b, column_c)" string with the SQL column names
    /// of the table.
    fn columnNames(comptime self: Table) []const u8 {
        comptime var s: []const u8 = "(";
        comptime var sep: []const u8 = "";
        inline for (self.columns) |column| {
            s = s ++ sep ++ column.name;
            sep = ", ";
        }
        s = s ++ ")";
        return s;
    }

    /// placeholders returns a "(?, ?, ?)" string with SQL placeholders, as many as there
    /// are SQL columns.
    fn placeholders(comptime self: Table) []const u8 {
        comptime var s: []const u8 = "(";
        comptime var chunk: []const u8 = "?";
        inline for (self.columns) |_| {
            s = s ++ chunk;
            chunk = ", ?";
        }
        s = s ++ ")";
        return s;
    }

    /// gtfsColumnNamesToIndices creates a std.ComptimeStringMap mapping GTFS column names
    /// to indices into Table.columns.
    fn gtfsColumnNamesToIndices(comptime self: Table) type {
        const kv_type = struct { []const u8, usize };
        comptime var kvs: [self.columns.len]kv_type = undefined;
        inline for (self.columns, 0..) |col, i| {
            kvs[i] = .{ col.gtfsName(), i };
        }
        return std.ComptimeStringMap(usize, kvs);
    }
};

/// ParentImplication describes the existance of parent objects in GTFS for a particular table.
///
/// For example, a calendar exception from GTFS's calendar_dates table implies
/// the existance of a parent calendar, even if it wasn't defined in the calendar table.
/// Impuls doesn't allow for implicit objects, and an extra INSERT may be necessary to
/// ensure foreign key references remain valid.
const ParentImplication = struct {
    /// sql_table names the SQL table name in which implied entities need to be the create.
    sql_table: []const u8,

    /// sql_key names the primary key of the sql_table.
    sql_key: []const u8,

    /// gtfs_key names the
    gtfs_key: []const u8,
};

/// Column contains necessary information for mapping between GTFS and Impuls columns.
const Column = struct {
    /// name contains the Impuls (SQL) column name.
    name: [:0]const u8,

    /// gtfs_name contains the GTFS column name, only if that is different than the `name`.
    /// Use the `getName()` getter to a non-optional GTFS column name.
    gtfs_name: ?[:0]const u8 = null,

    /// convert_to_sql takes the raw GTFS column value (or "" if the column is missing)
    /// and a line_number to produce an equivalent Impuls (SQL) value.
    from_gtfs: fn ([]const u8, u32) InvalidValueT!ColumnValue = from_gtfs.asIs,

    /// convert_to_gtfs, if present, takes the raw SQL column value, and adjusts the value of
    /// the column, so that ColumnValue.ensureString() will be a vaild GTFS value.
    to_gtfs: ?fn (*ColumnValue) void = null,

    /// gtfsName returns the GTFS name of the column.
    inline fn gtfsName(comptime self: Column) [:0]const u8 {
        return if (self.gtfs_name) |gtfs_name| gtfs_name else self.name;
    }
};

/// ColumnValue represents a possible SQL column value.
const ColumnValue = union(enum) {
    /// Null represents a NULL SQL value.
    Null,

    /// Int represents an INTEGER SQL value.
    Int: i64,

    /// Float represents a REAL SQL value.
    Float: f64,

    /// BorrowedString represents a TEXT SQL value, borrowed from another source.
    BorrowedString: []const u8,

    /// OwnedString represents a TEXT SQL value, owned by this ColumnValue.
    OwnedString: BoundedString,

    /// null_ creates a ColumnValue containing an SQL NULL.
    inline fn null_() ColumnValue {
        return ColumnValue{ .Null = {} };
    }

    /// int creates a ColumnValue containing an SQL INTEGER.
    inline fn int(i: i64) ColumnValue {
        return ColumnValue{ .Int = i };
    }

    /// float creates a ColumnValue containing an SQL REAL.
    inline fn float(f: f64) ColumnValue {
        return ColumnValue{ .Float = f };
    }

    /// borrowed creates a ColumnValue contaning a borrowed SQL TEXT.
    inline fn borrowed(s: []const u8) ColumnValue {
        return ColumnValue{ .BorrowedString = s };
    }

    /// owned creates a ColumnValue contaning an owned SQL TEXT.
    inline fn owned(s: BoundedString) ColumnValue {
        return ColumnValue{ .OwnedString = s };
    }

    /// formatted creates a ColumnValue contaning an owned SQL TEXT from a format string
    /// and its arguments. See std.fmt.format.
    fn formatted(comptime fmt_: []const u8, args: anytype) !ColumnValue {
        var s = BoundedString.init(0) catch unreachable;
        var fbs = std.io.fixedBufferStream(&s.buffer);
        try fmt.format(fbs.writer(), fmt_, args);
        s.len = @intCast(fbs.pos);
        return ColumnValue{ .BorrowedString = s };
    }

    /// format prints the ColumnValue into the provided writer. This function makes it possible
    /// to format ColumnValues directly using `fmt.format("{}", .{column_value})`.
    pub fn format(self: ColumnValue, comptime _: []const u8, _: fmt.FormatOptions, writer: anytype) !void {
        try writer.writeAll("gtfs.ColumnValue{ ");
        switch (self) {
            .Null => try writer.writeAll(".Null = {}"),
            .Int => |i| try writer.print(".Int = {}", .{i}),
            .Float => |f| try writer.print(".Float = {}", .{f}),
            .BorrowedString => |s| try writer.print(".BorrowedString = \"{s}\"", .{s}),
            .OwnedString => |s| try writer.print(".OwnedString = \"{s}\"", .{s.slice()}),
        }
        try writer.writeAll(" }");
    }

    /// bind binds the current ColumnValue to a given placeholder (by a one-based index) in
    /// the given SQLite Statement.
    ///
    /// It is the callers responsibility to ensure that text values fulfill SQLite's lifetime
    /// requirements. In order to ensure owned strings aren't reallocated, the ColumnValue must
    /// be passed by reference.
    fn bind(self: *const ColumnValue, stmt: sqlite3.Statement, placeholderOneIndex: c_int) !void {
        switch (self.*) {
            .Null => try stmt.bind(placeholderOneIndex, null),
            .Int => |i| try stmt.bind(placeholderOneIndex, i),
            .Float => |f| try stmt.bind(placeholderOneIndex, f),
            .BorrowedString => |s| try stmt.bind(placeholderOneIndex, s),
            .OwnedString => |*s| try stmt.bind(placeholderOneIndex, s.slice()),
        }
    }

    /// scan creates an appropriate ColumnValue for a given column of an executed SQLite statement.
    /// The result is never an owned string. Borrowed strings live until the statement is reset,
    /// advanced, or destroyed.
    fn scan(stmt: sqlite3.Statement, columnZeroBasedIndex: c_int) ColumnValue {
        return switch (stmt.columnType(columnZeroBasedIndex)) {
            .Integer => {
                var i: i64 = undefined;
                stmt.column(columnZeroBasedIndex, &i);
                ColumnValue.int(i);
            },

            .Float => {
                var f: f64 = undefined;
                stmt.column(columnZeroBasedIndex, &f);
                ColumnValue.float(f);
            },

            .Text, .Blob => {
                var s: []const u8 = undefined;
                stmt.column(columnZeroBasedIndex, &s);
                ColumnValue.borrowed(s);
            },

            .Null => ColumnValue.null_(),
        };
    }

    /// ensureString attempts to convert the stored value into a string, and returns it.
    ///
    /// If the result is a borrowed or an owned string, that string is directly returned,
    /// If the result is null, "" is returned. In both of those cases, the value remains unchanged.
    /// Otherwise (Int/Float), the ColumnValue is converted to an OwnedString first.
    fn ensureString(self: *ColumnValue) ![]const u8 {
        switch (self.*) {
            .Null => return "",

            .Int => |i| {
                self.* = try ColumnValue.formatted("{}", .{i});
                return self.OwnedString.slice();
            },

            .Float => |f| {
                self.* = try ColumnValue.formatted("{}", .{f});
                return self.OwnedString.slice();
            },

            .BorrowedString => |s| return s,

            .OwnedString => return self.OwnedString.slice(),
        }
    }
};

/// InvalidValue is the error returned by from_gtfs helpers to mark invalid values.
const InvalidValue = error.InvalidValue;

/// InvalidValueT is the type of `InvalidValue`, for use in helper function return types.
const InvalidValueT = @TypeOf(InvalidValue);

/// BoundedString is the type of ColumnValue.OwnedString - a bounded u8 array.
const BoundedString = std.BoundedArray(u8, 32);

/// from_gtfs contains helper functions for converting GTFS columns to Impuls/SQL columns.
const from_gtfs = struct {
    fn asIs(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        return ColumnValue.borrowed(s);
    }

    fn optional(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        return if (s.len == 0) ColumnValue.null_() else ColumnValue.borrowed(s);
    }

    fn int(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        const i = fmt.parseInt(i64, s, 10) catch return InvalidValue;
        return ColumnValue.int(i);
    }

    fn optionalInt(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
        return if (s.len == 0) ColumnValue.null_() else int(s, line_no);
    }

    fn intFallbackZero(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
        return if (s.len == 0) ColumnValue.int(0) else int(s, line_no);
    }

    fn float(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        const f = fmt.parseFloat(f64, s) catch return InvalidValue;
        return ColumnValue.float(f);
    }

    fn optionalFloat(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
        return if (s.len == 0) ColumnValue.null_() else float(s, line_no);
    }

    fn maybeWithZeroUnknown(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        if (s.len == 0) return ColumnValue.null_();
        if (s.len > 1) return InvalidValue;
        switch (s[0]) {
            '0' => return ColumnValue.null_(),
            '1' => return ColumnValue.int(1),
            '2' => return ColumnValue.int(0),
            else => return InvalidValue,
        }
    }

    fn maybeWithZeroFalse(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        if (s.len == 0) return ColumnValue.null_();
        if (s.len > 1) return InvalidValue;
        switch (s[0]) {
            '0' => return ColumnValue.int(0),
            '1' => return ColumnValue.int(1),
            else => return InvalidValue,
        }
    }

    fn date(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        // We could do more strict checks, but for now [0-9]{8} seems ok
        if (s.len != 8) return InvalidValue;
        for (s) |c| {
            if (!isDigit(c)) return InvalidValue;
        }

        var withDashes = BoundedString.init(10) catch unreachable;
        withDashes.buffer[0] = s[0];
        withDashes.buffer[1] = s[1];
        withDashes.buffer[2] = s[2];
        withDashes.buffer[3] = s[3];
        withDashes.buffer[4] = '-';
        withDashes.buffer[5] = s[4];
        withDashes.buffer[6] = s[5];
        withDashes.buffer[7] = '-';
        withDashes.buffer[8] = s[6];
        withDashes.buffer[9] = s[7];

        return ColumnValue.owned(withDashes);
    }

    fn optionalDate(s: []const u8, i: u32) InvalidValueT!ColumnValue {
        return if (s.len == 0) return ColumnValue.null_() else date(s, i);
    }

    fn time(str: []const u8, _: u32) InvalidValueT!ColumnValue {
        var parts = std.mem.splitScalar(u8, str, ':');
        const h_str = parts.next() orelse return InvalidValue;
        const m_str = parts.next() orelse return InvalidValue;
        const s_str = parts.next() orelse return InvalidValue;
        if (parts.next() != null) return InvalidValue;

        const h = fmt.parseUnsigned(u32, h_str, 10) catch return InvalidValue;
        const m = fmt.parseUnsigned(u32, m_str, 10) catch return InvalidValue;
        const s = fmt.parseUnsigned(u32, s_str, 10) catch return InvalidValue;

        return ColumnValue.int(@intCast(h * 3600 + m * 60 + s));
    }

    fn routeType(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        const extended_type = fmt.parseUnsigned(u16, s, 10) catch return InvalidValue;
        const normalized_type = switch (extended_type) {
            0...7, 11, 12 => extended_type,
            100...199 => 2, // railway service
            200...299 => 3, // coach service
            405 => 12, // monorail service
            400...404, 406...499 => 1, // urban railway service
            700...799 => 3, // bus service
            800...899 => 11, // trolleybus service
            900...999 => 0, // tram service
            1000...1199 => 4, // water service
            1200...1299 => 4, // ferry service
            1300...1399 => 6, // aerial lift service
            1400...1499 => 7, // funicular service
            else => return InvalidValue,
        };
        return ColumnValue.int(normalized_type);
    }

    fn agencyId(s: []const u8, _: u32) InvalidValueT!ColumnValue {
        return ColumnValue.borrowed(if (s.len == 0) "(missing)" else s);
    }

    fn attributionId(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
        if (s.len == 0) {
            // If there is no attribution_id, generate one from the line_no.
            // This doesn't guarantee uniqueness; but the assumption is that either
            // all attributions have an ID (and this path is not used),
            // or no attributions have an ID (and the line_no is unique).
            return ColumnValue.int(@intCast(line_no));
        }
        return ColumnValue.borrowed(s);
    }

    fn feedInfoId(_: []const u8, _: u32) InvalidValueT!ColumnValue {
        return ColumnValue.int(0);
    }
};

/// to_gtfs contains helper functions for converting Impuls/SQL columns to GTFS columns.
const to_gtfs = struct {
    fn date(v: *ColumnValue) void {
        switch (v.*) {
            .BorrowedString => |old| {
                // TODO: Verify the `old` string?
                var new = BoundedString.init(8) catch unreachable;
                new.buffer[0] = old[0];
                new.buffer[1] = old[1];
                new.buffer[2] = old[2];
                new.buffer[3] = old[3];
                new.buffer[4] = old[5];
                new.buffer[5] = old[6];
                new.buffer[6] = old[8];
                new.buffer[7] = old[9];
                v.* = ColumnValue.owned(new);
            },

            // TODO: What about OwnedString?

            .Null => {}, // allow optional values

            else => panic("invalid date value: {}", .{v.*}),
        }
    }

    fn time(v: *ColumnValue) void {
        switch (v.*) {
            .Int => |total_seconds| {
                assert(total_seconds > 0); // time can't be negative
                const s = @rem(total_seconds, 60);
                const total_minutes = @divTrunc(total_seconds, 60);
                const m = @rem(total_minutes, 60);
                const h = @divTrunc(total_minutes, 60);

                v.* = ColumnValue.formatted("{d:0>2}:{d:0>2}:{d:0>2}", .{ h, m, s }) catch |err| {
                    panic("failed to format time value {}: {}", .{ total_seconds, err });
                };
            },

            else => panic("invalid time value: {}", .{v.*}),
        }
    }

    fn maybeWithZeroUnknown(v: *ColumnValue) void {
        switch (v.*) {
            .Int => |i| v.* = ColumnValue.int(if (i != 0) 1 else 2),
            .Null => v.* = ColumnValue.int(0),
            else => {},
        }
    }
};

/// tables lists all known Table mappings between GTFS and Impuls models.
const tables = [_]Table{
    Table{
        .gtfs_name = "agency.txt",
        .sql_name = "agencies",
        .columns = &[_]Column{
            Column{ .name = "agency_id", .from_gtfs = from_gtfs.agencyId },
            Column{ .name = "name", .gtfs_name = "agency_name" },
            Column{ .name = "url", .gtfs_name = "agency_url" },
            Column{ .name = "timezone", .gtfs_name = "agency_timezone" },
            Column{ .name = "lang", .gtfs_name = "agency_lang" },
            Column{ .name = "phone", .gtfs_name = "agency_phone" },
            Column{ .name = "fare_url", .gtfs_name = "agency_fare_url" },
        },
    },
    Table{
        .gtfs_name = "attributions.txt",
        .sql_name = "attributions",
        .columns = &[_]Column{
            Column{ .name = "attribution_id", .from_gtfs = from_gtfs.attributionId },
            Column{ .name = "organization_name" },
            Column{ .name = "is_producer", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "is_operator", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "is_authority", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "is_data_source", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "url", .gtfs_name = "attribution_url" },
            Column{ .name = "email", .gtfs_name = "attribution_email" },
            Column{ .name = "phone", .gtfs_name = "attribution_phone" },
        },
    },
    Table{
        .gtfs_name = "calendar.txt",
        .sql_name = "calendars",
        .columns = &[_]Column{
            Column{ .name = "calendar_id", .gtfs_name = "service_id" },
            Column{ .name = "monday", .from_gtfs = from_gtfs.int },
            Column{ .name = "tuesday", .from_gtfs = from_gtfs.int },
            Column{ .name = "wednesday", .from_gtfs = from_gtfs.int },
            Column{ .name = "thursday", .from_gtfs = from_gtfs.int },
            Column{ .name = "friday", .from_gtfs = from_gtfs.int },
            Column{ .name = "saturday", .from_gtfs = from_gtfs.int },
            Column{ .name = "sunday", .from_gtfs = from_gtfs.int },
            Column{ .name = "start_date", .from_gtfs = from_gtfs.date, .to_gtfs = to_gtfs.date },
            Column{ .name = "end_date", .from_gtfs = from_gtfs.date, .to_gtfs = to_gtfs.date },
            Column{ .name = "desc", .gtfs_name = "service_desc" },
        },
    },
    Table{
        .gtfs_name = "calendar_dates.txt",
        .sql_name = "calendar_exceptions",
        .columns = &[_]Column{
            Column{ .name = "calendar_id", .gtfs_name = "service_id" },
            Column{ .name = "date", .from_gtfs = from_gtfs.date, .to_gtfs = to_gtfs.date },
            Column{ .name = "exception_type", .from_gtfs = from_gtfs.int },
        },
        .parent_implication = ParentImplication{
            .sql_table = "calendars",
            .sql_key = "calendar_id",
            .gtfs_key = "service_id",
        },
    },
    Table{
        .gtfs_name = "feed_info.txt",
        .sql_name = "feed_info",
        .columns = &[_]Column{
            Column{
                .name = "feed_info_id",
                .gtfs_name = "",
                .from_gtfs = from_gtfs.feedInfoId,
            },
            Column{ .name = "publisher_name", .gtfs_name = "feed_publisher_name" },
            Column{ .name = "publisher_url", .gtfs_name = "feed_publisher_url" },
            Column{ .name = "lang", .gtfs_name = "feed_lang" },
            Column{ .name = "version", .gtfs_name = "feed_version" },
            Column{ .name = "contact_email", .gtfs_name = "feed_contact_email" },
            Column{ .name = "contact_url", .gtfs_name = "feed_contact_url" },
            Column{ .name = "start_date", .gtfs_name = "feed_start_date", .from_gtfs = from_gtfs.optionalDate, .to_gtfs = to_gtfs.date },
            Column{
                .name = "end_date",
                .gtfs_name = "feed_end_date",
                .from_gtfs = from_gtfs.optionalDate,
                .to_gtfs = to_gtfs.date,
            },
        },
    },
    Table{
        .gtfs_name = "routes.txt",
        .sql_name = "routes",
        .columns = &[_]Column{
            Column{ .name = "route_id" },
            Column{ .name = "agency_id", .from_gtfs = from_gtfs.agencyId },
            Column{ .name = "short_name", .gtfs_name = "route_short_name" },
            Column{ .name = "long_name", .gtfs_name = "route_long_name" },
            Column{ .name = "type", .gtfs_name = "route_type", .from_gtfs = from_gtfs.routeType },
            Column{ .name = "color", .gtfs_name = "route_color" },
            Column{ .name = "text_color", .gtfs_name = "route_text_color" },
            Column{ .name = "sort_order", .gtfs_name = "route_sort_order", .from_gtfs = from_gtfs.optionalInt },
        },
    },
    Table{
        .gtfs_name = "stops.txt",
        .sql_name = "stops",
        .columns = &[_]Column{
            Column{ .name = "stop_id" },
            Column{ .name = "name", .gtfs_name = "stop_name" },
            Column{ .name = "lat", .gtfs_name = "stop_lat", .from_gtfs = from_gtfs.float },
            Column{ .name = "lon", .gtfs_name = "stop_lon", .from_gtfs = from_gtfs.float },
            Column{ .name = "code", .gtfs_name = "stop_code" },
            Column{ .name = "zone_id", .gtfs_name = "zone_id" },
            Column{ .name = "location_type", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "parent_station", .from_gtfs = from_gtfs.optional },
            Column{
                .name = "wheelchair_boarding",
                .from_gtfs = from_gtfs.maybeWithZeroUnknown,
                .to_gtfs = to_gtfs.maybeWithZeroUnknown,
            },
            Column{ .name = "platform_code" },
        },
    },
    Table{
        .gtfs_name = "fare_attributes.txt",
        .sql_name = "fare_attributes",
        .columns = &[_]Column{
            Column{ .name = "fare_id" },
            Column{ .name = "price", .from_gtfs = from_gtfs.float },
            Column{ .name = "currency_type" },
            Column{ .name = "payment_method", .from_gtfs = from_gtfs.int },
            Column{ .name = "transfers", .from_gtfs = from_gtfs.optionalInt },
            Column{ .name = "agency_id", .from_gtfs = from_gtfs.agencyId },
            Column{ .name = "transfer_duration", .from_gtfs = from_gtfs.optionalInt },
        },
    },
    Table{
        .gtfs_name = "fare_rules.txt",
        .sql_name = "fare_rules",
        .columns = &[_]Column{
            Column{ .name = "fare_id" },
            Column{ .name = "route_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "origin_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "destination_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "contains_id", .from_gtfs = from_gtfs.optional },
        },
    },
    Table{
        .gtfs_name = "shapes.txt",
        .sql_name = "shape_points",
        .columns = &[_]Column{
            Column{ .name = "shape_id" },
            Column{
                .name = "sequence",
                .gtfs_name = "shape_pt_sequence",
                .from_gtfs = from_gtfs.int,
            },
            Column{ .name = "lat", .gtfs_name = "shape_pt_lat", .from_gtfs = from_gtfs.float },
            Column{ .name = "lon", .gtfs_name = "shape_pt_lon", .from_gtfs = from_gtfs.float },
            Column{ .name = "shape_dist_traveled", .from_gtfs = from_gtfs.optionalFloat },
        },
        .parent_implication = ParentImplication{
            .sql_table = "shapes",
            .sql_key = "shape_id",
            .gtfs_key = "shape_id",
        },
    },
    Table{
        .gtfs_name = "trips.txt",
        .sql_name = "trips",
        .columns = &[_]Column{
            Column{ .name = "trip_id" },
            Column{ .name = "route_id" },
            Column{ .name = "calendar_id", .gtfs_name = "service_id" },
            Column{ .name = "headsign", .gtfs_name = "trip_headsign" },
            Column{ .name = "short_name", .gtfs_name = "trip_short_name" },
            Column{ .name = "direction", .gtfs_name = "direction_id", .from_gtfs = from_gtfs.optionalInt },
            Column{ .name = "block_id" },
            Column{ .name = "shape_id", .from_gtfs = from_gtfs.optional },
            Column{
                .name = "wheelchair_accessible",
                .from_gtfs = from_gtfs.maybeWithZeroUnknown,
                .to_gtfs = to_gtfs.maybeWithZeroUnknown,
            },
            Column{
                .name = "bikes_allowed",
                .from_gtfs = from_gtfs.maybeWithZeroUnknown,
                .to_gtfs = to_gtfs.maybeWithZeroUnknown,
            },
            Column{
                .name = "exceptional",
                .from_gtfs = from_gtfs.maybeWithZeroUnknown,
                .to_gtfs = to_gtfs.maybeWithZeroUnknown,
            },
        },
    },
    Table{
        .gtfs_name = "stop_times.txt",
        .sql_name = "stop_times",
        .columns = &[_]Column{
            Column{ .name = "trip_id" },
            Column{ .name = "stop_id" },
            Column{ .name = "stop_sequence", .from_gtfs = from_gtfs.int },
            Column{ .name = "arrival_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "departure_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "pickup_type", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "drop_off_type", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "stop_headsign" },
            Column{ .name = "shape_dist_traveled", .from_gtfs = from_gtfs.optionalFloat },
            Column{ .name = "original_stop_id" },
            Column{ .name = "platform" },
        },
    },
    Table{
        .gtfs_name = "frequencies.txt",
        .sql_name = "frequencies",
        .columns = &[_]Column{
            Column{ .name = "trip_id", .gtfs_name = "trip_id" },
            Column{ .name = "start_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "end_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "headway", .gtfs_name = "headway_secs", .from_gtfs = from_gtfs.int },
            Column{ .name = "exact_times", .from_gtfs = from_gtfs.intFallbackZero },
        },
    },
    Table{
        .gtfs_name = "transfers.txt",
        .sql_name = "transfers",
        .columns = &[_]Column{
            Column{ .name = "from_stop_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "to_stop_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "from_route_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "to_route_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "from_trip_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "to_trip_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "transfer_type", .from_gtfs = from_gtfs.int },
            Column{ .name = "min_transfer_time", .from_gtfs = from_gtfs.optionalInt },
        },
    },
};
