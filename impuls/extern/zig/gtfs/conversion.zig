const std = @import("std");
const sqlite3 = @import("../sqlite3.zig");

/// ColumnValue represents a possible SQL column value.
pub const ColumnValue = union(enum) {
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
    pub inline fn null_() ColumnValue {
        return ColumnValue{ .Null = {} };
    }

    /// int creates a ColumnValue containing an SQL INTEGER.
    pub inline fn int(i: i64) ColumnValue {
        return ColumnValue{ .Int = i };
    }

    /// float creates a ColumnValue containing an SQL REAL.
    pub inline fn float(f: f64) ColumnValue {
        return ColumnValue{ .Float = f };
    }

    /// borrowed creates a ColumnValue contaning a borrowed SQL TEXT.
    pub inline fn borrowed(s: []const u8) ColumnValue {
        return ColumnValue{ .BorrowedString = s };
    }

    /// owned creates a ColumnValue contaning an owned SQL TEXT.
    pub inline fn owned(s: BoundedString) ColumnValue {
        return ColumnValue{ .OwnedString = s };
    }

    /// formatted creates a ColumnValue contaning an owned SQL TEXT from a format string
    /// and its arguments. See std.fmt.format.
    pub fn formatted(comptime fmt_: []const u8, args: anytype) !ColumnValue {
        var s = BoundedString.init(0) catch unreachable;
        var fbs = std.io.fixedBufferStream(&s.buffer);
        try std.fmt.format(fbs.writer(), fmt_, args);
        s.len = @intCast(fbs.pos);
        return ColumnValue.owned(s);
    }

    /// format prints the ColumnValue into the provided writer. This function makes it possible
    /// to format ColumnValues directly using `fmt.format("{}", .{column_value})`.
    pub fn format(self: ColumnValue, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
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
    pub fn bind(self: *const ColumnValue, stmt: sqlite3.Statement, placeholderOneIndex: c_int) !void {
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
    pub fn scan(stmt: sqlite3.Statement, columnZeroBasedIndex: c_int) ColumnValue {
        switch (stmt.columnType(columnZeroBasedIndex)) {
            .Integer => {
                var i: i64 = undefined;
                stmt.column(columnZeroBasedIndex, &i);
                return ColumnValue.int(i);
            },

            .Float => {
                var f: f64 = undefined;
                stmt.column(columnZeroBasedIndex, &f);
                return ColumnValue.float(f);
            },

            .Text, .Blob => {
                var s: []const u8 = undefined;
                stmt.column(columnZeroBasedIndex, &s);
                return ColumnValue.borrowed(s);
            },

            .Null => return ColumnValue.null_(),
        }
    }

    /// ensureString attempts to convert the stored value into a string, and returns it.
    ///
    /// If the result is a borrowed or an owned string, that string is directly returned,
    /// If the result is null, "" is returned. In both of those cases, the value remains unchanged.
    /// Otherwise (Int/Float), the ColumnValue is converted to an OwnedString first.
    pub fn ensureString(self: *ColumnValue) ![]const u8 {
        switch (self.*) {
            .Null => return "",

            .Int => |i| {
                self.* = try ColumnValue.formatted("{d}", .{i});
                return self.OwnedString.slice();
            },

            .Float => |f| {
                self.* = try ColumnValue.formatted("{d}", .{f});
                return self.OwnedString.slice();
            },

            .BorrowedString => |s| return s,

            .OwnedString => return self.OwnedString.slice(),
        }
    }
};

/// InvalidValue is the error returned by from_gtfs helpers to mark invalid values.
pub const InvalidValue = error.InvalidValue;

/// InvalidValueT is the type of `InvalidValue`, for use in helper function return types.
pub const InvalidValueT = @TypeOf(InvalidValue);

/// BoundedString is the type of ColumnValue.OwnedString - a bounded u8 array.
pub const BoundedString = std.BoundedArray(u8, 32);

/// FnFromGtfs is a type alias for a function converting data from GTFS to Impuls.
/// The two arguments represent the value coming in from the CSV table and line number.
pub const FnFromGtfs = *const fn ([]const u8, u32) InvalidValueT!ColumnValue;

/// FnToGtfs is a tyle aliast for a function adjusting data coming from Impuls to GTFS.
pub const FnToGtfs = *const fn (*ColumnValue) void;
