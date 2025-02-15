// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const c = @import("./conversion.zig");
const std = @import("std");

const BoundedString = c.BoundedString;
const ColumnValue = c.ColumnValue;
const fmt = std.fmt;
const InvalidValue = c.InvalidValue;
const InvalidValueT = c.InvalidValueT;

/// asIs borrows the provided string.
pub fn asIs(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    return ColumnValue.borrowed(s);
}

test "gtfs.conversion_from_gtfs.asIs" {
    const v = try asIs("foo", 1);
    try std.testing.expectEqualStrings("foo", v.BorrowedString);
}

/// optional returns Null if `s` empty, and borrows `s` otherwise.
pub fn optional(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.null_() else ColumnValue.borrowed(s);
}

test "gtfs.conversion_from_gtfs.optional" {
    var v = try optional("foo", 1);
    try std.testing.expectEqualStrings("foo", v.BorrowedString);

    v = try optional("", 1);
    try std.testing.expectEqualStrings("Null", @tagName(v));
}

/// int returns the value of `s` as a base-10 integer, raising InvalidValue if `s` is not
/// a valid number.
pub fn int(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    const i = fmt.parseInt(i64, s, 10) catch return InvalidValue;
    return ColumnValue.int(i);
}

test "gtfs.conversion_from_gtfs.int" {
    const v = try int("42", 1);
    try std.testing.expectEqual(@as(i64, 42), v.Int);

    try std.testing.expectError(InvalidValue, int("", 1));
    try std.testing.expectError(InvalidValue, int("foo", 1));
}

/// optionalInt returns Null if `s` is empty, the value of `s` as a base-10 integer
/// or raises InvalidValue if `s` is not a valid number.
pub fn optionalInt(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.null_() else int(s, line_no);
}

test "gtfs.conversion_from_gtfs.optionalInt" {
    var v = try optionalInt("42", 1);
    try std.testing.expectEqual(@as(i64, 42), v.Int);

    v = try optionalInt("", 1);
    try std.testing.expectEqualStrings("Null", @tagName(v));

    try std.testing.expectError(InvalidValue, optionalInt("foo", 1));
}

/// intFallbackZero returns 0 if `s` is empty, the value of `s` as a base-10 integer
/// or raises InvalidValue if `s` is not a valid number.
pub fn intFallbackZero(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.int(0) else int(s, line_no);
}

test "gtfs.conversion_from_gtfs.intFallbackZero" {
    var v = try intFallbackZero("42", 1);
    try std.testing.expectEqual(@as(i64, 42), v.Int);

    v = try intFallbackZero("", 1);
    try std.testing.expectEqual(@as(i64, 0), v.Int);

    try std.testing.expectError(InvalidValue, intFallbackZero("foo", 1));
}

/// float returns the value of `s` as a floating-point number,
/// raising InvalidValue if `s` is not a valid number.
pub fn float(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    const f = fmt.parseFloat(f64, s) catch return InvalidValue;
    return ColumnValue.float(f);
}

test "gtfs.conversion_from_gtfs.float" {
    const v = try float("-3.1415", 1);
    try std.testing.expectEqual(@as(f64, -3.1415), v.Float);

    try std.testing.expectError(InvalidValue, float("", 1));
    try std.testing.expectError(InvalidValue, float("foo", 1));
}

/// optionalFloat returns Null if `s` is empty, the value of `s` as a floating-point number,
/// or raises InvalidValue if `s` is not a valid number.
pub fn optionalFloat(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.null_() else float(s, line_no);
}

test "gtfs.conversion_from_gtfs.optionalFloat" {
    var v = try optionalFloat("-3.1415", 1);
    try std.testing.expectEqual(@as(f64, -3.1415), v.Float);

    v = try optionalFloat("", 1);
    try std.testing.expectEqualStrings("Null", @tagName(v));

    try std.testing.expectError(InvalidValue, optionalFloat("foo", 1));
}

/// maybeWithZeroUnknown parses an optional flag from a GTFS tri-state enum.
/// "0" and "" is parsed as Null, "1" as true (1), "2" as false (0).
/// Any other value causes InvalidValue to be raised.
pub fn maybeWithZeroUnknown(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    if (s.len == 0) return ColumnValue.null_();
    if (s.len > 1) return InvalidValue;
    switch (s[0]) {
        '0' => return ColumnValue.null_(),
        '1' => return ColumnValue.int(1),
        '2' => return ColumnValue.int(0),
        else => return InvalidValue,
    }
}

test "gtfs.conversion_from_gtfs.maybeWithZeroUnknown" {
    var v = try maybeWithZeroUnknown("", 1);
    try std.testing.expectEqualStrings("Null", @tagName(v));

    v = try maybeWithZeroUnknown("0", 1);
    try std.testing.expectEqualStrings("Null", @tagName(v));

    v = try maybeWithZeroUnknown("1", 1);
    try std.testing.expectEqual(@as(i64, 1), v.Int);

    v = try maybeWithZeroUnknown("2", 1);
    try std.testing.expectEqual(@as(i64, 0), v.Int);

    try std.testing.expectError(InvalidValue, maybeWithZeroUnknown("3", 1));
    try std.testing.expectError(InvalidValue, maybeWithZeroUnknown("foo", 1));
}

/// maybeWithZeroUnknown parses an optional flag from a GTFS boolean flag.
/// "" is parsed as Null, "0" as false (0) and "1" as true (1).
/// Any other value causes InvalidValue to be raised.
pub fn maybeWithZeroFalse(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    if (s.len == 0) return ColumnValue.null_();
    if (s.len > 1) return InvalidValue;
    switch (s[0]) {
        '0' => return ColumnValue.int(0),
        '1' => return ColumnValue.int(1),
        else => return InvalidValue,
    }
}

test "gtfs.conversion_from_gtfs.maybeWithZeroFalse" {
    var v = try maybeWithZeroFalse("", 1);
    try std.testing.expectEqualStrings("Null", @tagName(v));

    v = try maybeWithZeroFalse("0", 1);
    try std.testing.expectEqual(@as(i64, 0), v.Int);

    v = try maybeWithZeroFalse("1", 1);
    try std.testing.expectEqual(@as(i64, 1), v.Int);

    try std.testing.expectError(InvalidValue, maybeWithZeroFalse("2", 1));
    try std.testing.expectError(InvalidValue, maybeWithZeroFalse("foo", 1));
}

/// date transforms a "YYYYMMDD" string into an owned "YYYY-MM-DD" string.
pub fn date(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    // We could do more strict checks, but for now [0-9]{8} seems ok
    if (s.len != 8) return InvalidValue;
    for (s) |octet| {
        if (!std.ascii.isDigit(octet)) return InvalidValue;
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

test "gtfs.conversion_from_gtfs.date" {
    var v = try date("20200520", 1);
    try std.testing.expectEqualStrings("2020-05-20", v.OwnedString.slice());

    try std.testing.expectError(InvalidValue, date("", 1));
    try std.testing.expectError(InvalidValue, date("foo", 1));
}

/// optionalDate returns Null if `s` is empty, transforming "YYYYMMDD" strings into
/// owned "YYYY-MM-DD" strings otherwise.
pub fn optionalDate(s: []const u8, i: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) return ColumnValue.null_() else date(s, i);
}

test "gtfs.conversion_from_gtfs.optionalDate" {
    var v = try optionalDate("20200520", 1);
    try std.testing.expectEqualStrings("2020-05-20", v.OwnedString.slice());

    v = try optionalDate("", 1);
    try std.testing.expectEqualStrings("Null", @tagName(v));

    try std.testing.expectError(InvalidValue, optionalDate("foo", 1));
}

/// time parses "HH:MM:SS" GTFS time strings into a total number of seconds (an integer).
pub fn time(str: []const u8, _: u32) InvalidValueT!ColumnValue {
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

test "gtfs.conversion_from_gtfs.time" {
    const v = try time("12:15:30", 1);
    try std.testing.expectEqual(@as(i64, 12 * 3600 + 15 * 60 + 30), v.Int);

    try std.testing.expectError(InvalidValue, time("", 1));
    try std.testing.expectError(InvalidValue, time("foo", 1));
    try std.testing.expectError(InvalidValue, time("12:15:30:00", 1));
    try std.testing.expectError(InvalidValue, time("12:15:aa", 1));
}

/// routeType converts possibly-extended GTFS route types into simplified Impuls route types.
/// If `s` is not a valid integer, or not a recognized route type, InvalidValue is raised.
pub fn routeType(s: []const u8, _: u32) InvalidValueT!ColumnValue {
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

test "gtfs.conversion_from_gtfs.routeType" {
    var v = try routeType("3", 1);
    try std.testing.expectEqual(@as(i64, 3), v.Int);

    v = try routeType("700", 1);
    try std.testing.expectEqual(@as(i64, 3), v.Int);

    try std.testing.expectError(InvalidValue, time("", 1));
    try std.testing.expectError(InvalidValue, time("8", 1));
    try std.testing.expectError(InvalidValue, time("foo", 1));
}

/// agencyId borrows `s` if it's not empty, otherwise borrows "(missing)".
pub fn agencyId(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    return ColumnValue.borrowed(if (s.len == 0) "(missing)" else s);
}

test "gtfs.conversion_from_gtfs.agencyId" {
    var v = try agencyId("foo", 1);
    try std.testing.expectEqualStrings("foo", v.BorrowedString);

    v = try agencyId("", 1);
    try std.testing.expectEqualStrings("(missing)", v.BorrowedString);
}

/// attributionId returns `s` if it's not empty, otherwise returns the provided line_no.
pub fn attributionId(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
    if (s.len == 0) {
        // If there is no attribution_id, generate one from the line_no.
        // This doesn't guarantee uniqueness; but the assumption is that either
        // all attributions have an ID (and this path is not used),
        // or no attributions have an ID (and the line_no is unique).
        return ColumnValue.int(@intCast(line_no));
    }
    return ColumnValue.borrowed(s);
}

test "gtfs.conversion_from_gtfs.attributionId" {
    var v = try attributionId("foo", 1);
    try std.testing.expectEqualStrings("foo", v.BorrowedString);

    v = try attributionId("", 1);
    try std.testing.expectEqual(@as(i64, 1), v.Int);
}

/// feedInfoId always returns 0.
pub fn feedInfoId(_: []const u8, _: u32) InvalidValueT!ColumnValue {
    return ColumnValue.int(0);
}

test "gtfs.conversion_from_gtfs.feedInfoId" {
    var v = try feedInfoId("foo", 1);
    try std.testing.expectEqual(@as(i64, 0), v.Int);

    v = try feedInfoId("", 1);
    try std.testing.expectEqual(@as(i64, 0), v.Int);
}
