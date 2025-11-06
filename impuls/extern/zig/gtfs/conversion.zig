// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const std = @import("std");
const sqlite3 = @import("../sqlite3.zig");
const BoundedArray = @import("../bounded_array.zig").BoundedArray;

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

    /// borrowed creates a ColumnValue containing a borrowed SQL TEXT.
    pub inline fn borrowed(s: []const u8) ColumnValue {
        return ColumnValue{ .BorrowedString = s };
    }

    /// owned creates a ColumnValue containing an owned SQL TEXT.
    pub inline fn owned(s: BoundedString) ColumnValue {
        return ColumnValue{ .OwnedString = s };
    }

    /// formatted creates a ColumnValue containing an owned SQL TEXT from a format string
    /// and its arguments. See std.Io.Writer.print.
    pub fn formatted(comptime fmt_: []const u8, args: anytype) !ColumnValue {
        var s = BoundedString{};
        var w = std.Io.Writer.fixed(&s.buffer);
        try w.print(fmt_, args);
        s.len = w.end;
        return ColumnValue.owned(s);
    }

    /// format prints the ColumnValue into the provided writer. This function makes it possible
    /// to format ColumnValues directly using `std.Io.Writer.print("{f}", .{column_value})`.
    pub fn format(self: ColumnValue, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.writeAll("gtfs.ColumnValue{ ");
        switch (self) {
            .Null => try writer.writeAll(".Null = {}"),
            .Int => |i| try writer.print(".Int = {}", .{i}),
            .Float => |f| try writer.print(".Float = {}", .{f}),
            .BorrowedString => |s| try writer.print(".BorrowedString = \"{s}\"", .{s}),
            .OwnedString => |s| try writer.print(".OwnedString = \"{s}\"", .{s.constSlice()}),
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
            .OwnedString => |*s| try stmt.bind(placeholderOneIndex, s.constSlice()),
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
pub const BoundedString = BoundedArray(u8, 32);

/// FnFromGtfs is a type alias for a function converting data from GTFS to Impuls.
/// The two arguments represent the value coming in from the CSV table and line number.
pub const FnFromGtfs = *const fn ([]const u8, u32) InvalidValueT!ColumnValue;

/// FnToGtfs is a tyle aliast for a function adjusting data coming from Impuls to GTFS.
pub const FnToGtfs = *const fn (*ColumnValue) void;

test "gtfs.conversion.ColumnValue.bind" {
    var db = try sqlite3.Connection.init(":memory:", .{});
    defer db.deinit();

    {
        var s = try db.prepare("SELECT ?");
        defer s.deinit();

        const v = ColumnValue.null_();
        try v.bind(s, 1);

        try std.testing.expect(try s.step());
        try std.testing.expectEqual(sqlite3.Datatype.Null, s.columnType(0));
    }

    {
        var s = try db.prepare("SELECT ?");
        defer s.deinit();

        const v = ColumnValue.int(42);
        try v.bind(s, 1);

        try std.testing.expect(try s.step());
        try std.testing.expectEqual(sqlite3.Datatype.Integer, s.columnType(0));

        var i: i64 = undefined;
        s.column(0, &i);
        try std.testing.expectEqual(@as(i64, 42), i);
    }

    {
        var s = try db.prepare("SELECT ?");
        defer s.deinit();

        const v = ColumnValue.float(-3.1415);
        try v.bind(s, 1);

        try std.testing.expect(try s.step());
        try std.testing.expectEqual(sqlite3.Datatype.Float, s.columnType(0));

        var f: f64 = undefined;
        s.column(0, &f);
        try std.testing.expectEqual(@as(f64, -3.1415), f);
    }

    {
        var s = try db.prepare("SELECT ?");
        defer s.deinit();

        const v = ColumnValue.borrowed("Foo Bar Baz");
        try v.bind(s, 1);

        try std.testing.expect(try s.step());
        try std.testing.expectEqual(sqlite3.Datatype.Text, s.columnType(0));

        var t: []const u8 = undefined;
        s.column(0, &t);
        try std.testing.expectEqualStrings("Foo Bar Baz", t);
    }

    {
        var s = try db.prepare("SELECT ?");
        defer s.deinit();

        var bs = BoundedString{};
        try bs.appendSlice("Foo Bar Baz");
        const v = ColumnValue.owned(bs);
        try v.bind(s, 1);

        try std.testing.expect(try s.step());
        try std.testing.expectEqual(sqlite3.Datatype.Text, s.columnType(0));

        var t: []const u8 = undefined;
        s.column(0, &t);
        try std.testing.expectEqualStrings("Foo Bar Baz", t);
    }
}

test "gtfs.conversion.ColumnValue.scan" {
    var db = try sqlite3.Connection.init(":memory:", .{});
    defer db.deinit();

    {
        var s = try db.prepare("SELECT 42");
        defer s.deinit();

        try std.testing.expectEqual(true, try s.step());
        const v = ColumnValue.scan(s, 0);
        try std.testing.expectEqual(@as(i64, 42), v.Int);
    }

    {
        var s = try db.prepare("SELECT -3.1415");
        defer s.deinit();

        try std.testing.expectEqual(true, try s.step());
        const v = ColumnValue.scan(s, 0);
        try std.testing.expectEqual(@as(f64, -3.1415), v.Float);
    }

    {
        var s = try db.prepare("SELECT 'Foo Bar Baz'");
        defer s.deinit();

        try std.testing.expectEqual(true, try s.step());
        const v = ColumnValue.scan(s, 0);
        try std.testing.expectEqualStrings("Foo Bar Baz", v.BorrowedString);
    }

    {
        var s = try db.prepare("SELECT NULL");
        defer s.deinit();

        try std.testing.expectEqual(true, try s.step());
        const v = ColumnValue.scan(s, 0);
        try std.testing.expectEqualStrings("Null", @tagName(v));
    }
}

test "gtfs.conversion.ColumnValue.ensureString" {
    var v = ColumnValue.null_();
    try std.testing.expectEqualStrings("", try v.ensureString());

    v = ColumnValue.int(42);
    try std.testing.expectEqualStrings("42", try v.ensureString());

    v = ColumnValue.float(-3.1415);
    try std.testing.expectEqualStrings("-3.1415", try v.ensureString());

    v = ColumnValue.borrowed("Foo Bar Baz");
    try std.testing.expectEqualStrings("Foo Bar Baz", try v.ensureString());

    var owned_str = BoundedString{};
    try owned_str.appendSlice("Foo Bar Baz");
    v = ColumnValue.owned(owned_str);
    try std.testing.expectEqualStrings("Foo Bar Baz", try v.ensureString());
}
