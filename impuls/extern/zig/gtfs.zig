const csv = @import("./csv.zig");
const std = @import("std");
const sqlite3 = @import("./sqlite3.zig");
const fmt = std.fmt;
const fs = std.fs;
const Allocator = std.mem.Allocator;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;
const bufferedReaderSize = std.io.bufferedReaderSize;
const isDigit = std.ascii.isDigit;
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

fn loadTable(db: sqlite3.Connection, gtfs_dir: fs.Dir, allocator: Allocator, comptime table: Table) !void {
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
                arguments[i] = col.convert_to_sql(raw_value, self.record.line_no) catch |err| {
                    print(
                        "{s}:{d}:{s}: {}\n",
                        .{ table.gtfs_name, self.record.line_no, col.gtfs_name, err },
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
            s = s ++ sep ++ column.sql_name;
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
            kvs[i] = .{ col.gtfs_name, i };
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
    /// gtfs_name contains the GTFS column name
    gtfs_name: [:0]const u8,

    /// sql_name contains the Impuls (SQL) column name
    sql_name: [:0]const u8,

    /// convert_to_sql takes the raw GTFS column value (or "" if the column is missing)
    /// and a line_number to produce an equivalent Impuls (SQL) value.
    convert_to_sql: fn ([]const u8, u32) InvalidValueT!ColumnValue,

    inline fn sameName(
        comptime name: [:0]const u8,
        comptime convert_to_sql: fn ([]const u8, u32) InvalidValueT!ColumnValue,
    ) Column {
        return Column{ .gtfs_name = name, .sql_name = name, .convert_to_sql = convert_to_sql };
    }

    inline fn sameNameAsIs(comptime name: [:0]const u8) Column {
        return Column{ .gtfs_name = name, .sql_name = name, .convert_to_sql = from_gtfs.asIs };
    }

    inline fn asIs(comptime gtfs_name: [:0]const u8, comptime sql_name: [:0]const u8) Column {
        return Column{ .gtfs_name = gtfs_name, .sql_name = sql_name, .convert_to_sql = from_gtfs.asIs };
    }

    inline fn init(
        comptime gtfs_name: [:0]const u8,
        comptime sql_name: [:0]const u8,
        comptime convert_to_sql: fn ([]const u8, u32) InvalidValueT!ColumnValue,
    ) Column {
        return Column{ .gtfs_name = gtfs_name, .sql_name = sql_name, .convert_to_sql = convert_to_sql };
    }
};

const ColumnValue = union(enum) {
    Null,
    Int: i64,
    Float: f64,
    BorrowedString: []const u8,
    OwnedString: BoundedString,

    inline fn null_() ColumnValue {
        return ColumnValue{ .Null = {} };
    }

    inline fn int(i: i64) ColumnValue {
        return ColumnValue{ .Int = i };
    }

    inline fn float(f: f64) ColumnValue {
        return ColumnValue{ .Float = f };
    }

    inline fn borrowed(s: []const u8) ColumnValue {
        return ColumnValue{ .BorrowedString = s };
    }

    inline fn owned(s: BoundedString) ColumnValue {
        return ColumnValue{ .OwnedString = s };
    }

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

    fn bind(self: *const ColumnValue, stmt: sqlite3.Statement, columnOneBasedIndex: c_int) !void {
        switch (self.*) {
            .Null => try stmt.bind(columnOneBasedIndex, null),
            .Int => |i| try stmt.bind(columnOneBasedIndex, i),
            .Float => |f| try stmt.bind(columnOneBasedIndex, f),
            .BorrowedString => |s| try stmt.bind(columnOneBasedIndex, s),
            .OwnedString => |*s| try stmt.bind(columnOneBasedIndex, s.slice()),
        }
    }
};

const InvalidValue = error.InvalidValue;
const InvalidValueT = @TypeOf(InvalidValue);
const BoundedString = std.BoundedArray(u8, 16);

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
        return if (s.len == 0) ColumnValue.borrowed("0") else int(s, line_no);
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

const to_gtfs = struct {
    fn as_is(s: []const u8) []const u8 {
        return s;
    }
};

const tables = [_]Table{
    Table{
        .gtfs_name = "agency.txt",
        .sql_name = "agencies",
        .columns = &[_]Column{
            Column.sameName("agency_id", from_gtfs.asIs),
            Column.asIs("agency_name", "name"),
            Column.asIs("agency_url", "url"),
            Column.asIs("agency_timezone", "timezone"),
            Column.asIs("agency_lang", "lang"),
            Column.asIs("agency_phone", "phone"),
            Column.asIs("agency_fare_url", "fare_url"),
        },
    },
    Table{
        .gtfs_name = "attributions.txt",
        .sql_name = "attributions",
        .columns = &[_]Column{
            Column.sameName("attribution_id", from_gtfs.attributionId),
            Column.sameNameAsIs("organization_name"),
            Column.sameName("is_producer", from_gtfs.intFallbackZero),
            Column.sameName("is_operator", from_gtfs.intFallbackZero),
            Column.sameName("is_authority", from_gtfs.intFallbackZero),
            Column.sameName("is_data_source", from_gtfs.intFallbackZero),
            Column.asIs("attribution_url", "url"),
            Column.asIs("attribution_email", "email"),
            Column.asIs("attribution_phone", "phone"),
        },
    },
    Table{
        .gtfs_name = "calendar.txt",
        .sql_name = "calendars",
        .columns = &[_]Column{
            Column.asIs("service_id", "calendar_id"),
            Column.sameName("monday", from_gtfs.int),
            Column.sameName("tuesday", from_gtfs.int),
            Column.sameName("wednesday", from_gtfs.int),
            Column.sameName("thursday", from_gtfs.int),
            Column.sameName("friday", from_gtfs.int),
            Column.sameName("saturday", from_gtfs.int),
            Column.sameName("sunday", from_gtfs.int),
            Column.sameName("start_date", from_gtfs.date),
            Column.sameName("end_date", from_gtfs.date),
            Column.asIs("service_desc", "desc"),
        },
    },
    Table{
        .gtfs_name = "calendar_dates.txt",
        .sql_name = "calendar_exceptions",
        .columns = &[_]Column{
            Column.asIs("service_id", "calendar_id"),
            Column.sameName("date", from_gtfs.date),
            Column.sameName("exception_type", from_gtfs.int),
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
            Column.init("", "feed_info_id", from_gtfs.feedInfoId),
            Column.asIs("feed_publisher_name", "publisher_name"),
            Column.asIs("feed_publisher_url", "publisher_url"),
            Column.asIs("feed_lang", "lang"),
            Column.asIs("feed_version", "version"),
            Column.asIs("feed_contact_email", "contact_email"),
            Column.asIs("feed_contact_url", "contact_url"),
            Column.init("feed_start_date", "start_date", from_gtfs.optionalDate),
            Column.init("feed_end_date", "end_date", from_gtfs.optionalDate),
        },
    },
    Table{
        .gtfs_name = "routes.txt",
        .sql_name = "routes",
        .columns = &[_]Column{
            Column.sameNameAsIs("route_id"),
            Column.sameName("agency_id", from_gtfs.agencyId),
            Column.asIs("route_short_name", "short_name"),
            Column.asIs("route_long_name", "long_name"),
            Column.init("route_type", "type", from_gtfs.routeType),
            Column.asIs("route_color", "color"),
            Column.asIs("route_text_color", "text_color"),
            Column.init(
                "route_sort_order",
                "sort_order",
                from_gtfs.optionalInt,
            ),
        },
    },
    Table{
        .gtfs_name = "stops.txt",
        .sql_name = "stops",
        .columns = &[_]Column{
            Column.sameNameAsIs("stop_id"),
            Column.asIs("stop_name", "name"),
            Column.init("stop_lat", "lat", from_gtfs.float),
            Column.init("stop_lon", "lon", from_gtfs.float),
            Column.asIs("stop_code", "code"),
            Column.asIs("zone_id", "zone_id"),
            Column.sameName("location_type", from_gtfs.intFallbackZero),
            Column.sameName("parent_station", from_gtfs.optional),
            Column.sameName("wheelchair_boarding", from_gtfs.maybeWithZeroUnknown),
            Column.sameNameAsIs("platform_code"),
        },
    },
    Table{
        .gtfs_name = "shapes.txt",
        .sql_name = "shape_points",
        .columns = &[_]Column{
            Column.sameNameAsIs("shape_id"),
            Column.init("shape_pt_sequence", "sequence", from_gtfs.int),
            Column.init("shape_pt_lat", "lat", from_gtfs.float),
            Column.init("shape_pt_lon", "lon", from_gtfs.float),
            Column.sameName("shape_dist_traveled", from_gtfs.optionalFloat),
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
            Column.sameNameAsIs("trip_id"),
            Column.sameNameAsIs("route_id"),
            Column.asIs("service_id", "calendar_id"),
            Column.asIs("trip_headsign", "headsign"),
            Column.asIs("trip_short_name", "short_name"),
            Column.init(
                "direction_id",
                "direction",
                from_gtfs.optionalInt,
            ),
            Column.sameNameAsIs("block_id"),
            Column.sameName("shape_id", from_gtfs.optional),
            Column.sameName("wheelchair_accessible", from_gtfs.maybeWithZeroUnknown),
            Column.sameName("bikes_allowed", from_gtfs.maybeWithZeroUnknown),
            Column.sameName("exceptional", from_gtfs.maybeWithZeroUnknown),
        },
    },
    Table{
        .gtfs_name = "stop_times.txt",
        .sql_name = "stop_times",
        .columns = &[_]Column{
            Column.sameNameAsIs("trip_id"),
            Column.sameNameAsIs("stop_id"),
            Column.sameName("stop_sequence", from_gtfs.int),
            Column.sameName("arrival_time", from_gtfs.time),
            Column.sameName("departure_time", from_gtfs.time),
            Column.sameName("pickup_type", from_gtfs.intFallbackZero),
            Column.sameName("drop_off_type", from_gtfs.intFallbackZero),
            Column.sameNameAsIs("stop_headsign"),
            Column.sameName("shape_dist_traveled", from_gtfs.optionalFloat),
            Column.sameNameAsIs("original_stop_id"),
            Column.sameNameAsIs("platform"),
        },
    },
    Table{
        .gtfs_name = "transfers.txt",
        .sql_name = "transfers",
        .columns = &[_]Column{
            Column.sameName("from_stop_id", from_gtfs.optional),
            Column.sameName("to_stop_id", from_gtfs.optional),
            Column.sameName("from_route_id", from_gtfs.optional),
            Column.sameName("to_route_id", from_gtfs.optional),
            Column.sameName("from_trip_id", from_gtfs.optional),
            Column.sameName("to_trip_id", from_gtfs.optional),
            Column.sameName("transfer_type", from_gtfs.int),
            Column.sameName("min_transfer_time", from_gtfs.optionalInt),
        },
    },
    Table{
        .gtfs_name = "fare_attributes.txt",
        .sql_name = "fare_attributes",
        .columns = &[_]Column{
            Column.sameNameAsIs("fare_id"),
            Column.sameName("price", from_gtfs.float),
            Column.sameNameAsIs("currency_type"),
            Column.sameName("payment_method", from_gtfs.int),
            Column.sameName("transfers", from_gtfs.optionalInt),
            Column.sameName("agency_id", from_gtfs.agencyId),
            Column.sameName("transfer_duration", from_gtfs.optionalInt),
        },
    },
    Table{
        .gtfs_name = "fare_rules.txt",
        .sql_name = "fare_rules",
        .columns = &[_]Column{
            Column.sameNameAsIs("fare_id"),
            Column.sameName("route_id", from_gtfs.optional),
            Column.sameName("origin_id", from_gtfs.optional),
            Column.sameName("destination_id", from_gtfs.optional),
            Column.sameName("contains_id", from_gtfs.optional),
        },
    },
};
