// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const c = @import("./conversion.zig");
const std = @import("std");

const assert = std.debug.assert;
const BoundedString = c.BoundedString;
const ColumnValue = c.ColumnValue;
const panic = std.debug.panic;

/// date ensures v holds an owned "YYYYMMDD" (GTFS-compliant) string.
pub fn date(v: *ColumnValue) void {
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

        .Null => {}, // allow optional values

        else => panic("invalid date value: {f}", .{v.*}),
    }
}

test "gtfs.conversion_to_gtfs.date" {
    var v = ColumnValue.borrowed("2024-03-15");
    date(&v);
    try std.testing.expectEqualStrings("20240315", try v.ensureString());

    v = ColumnValue.null_();
    try std.testing.expectEqualStrings("", try v.ensureString());
}

/// time converts an integer value (representing seconds-since-midnight) into a
/// "HH:MM:SS" borrowed string.
pub fn time(v: *ColumnValue) void {
    switch (v.*) {
        .Int => |total_seconds_signed| {
            assert(total_seconds_signed >= 0); // time can't be negative

            // we need to operate on unsigned numbers, otherwise plus signs are written,
            // giving values like "+5:+5:+0"
            const total_seconds: u64 = @intCast(total_seconds_signed);
            const s = total_seconds % 60;
            const total_minutes = total_seconds / 60;
            const m = total_minutes % 60;
            const h = total_minutes / 60;

            v.* = ColumnValue.formatted("{d:0>2}:{d:0>2}:{d:0>2}", .{ h, m, s }) catch |err| {
                panic("failed to format time value {}: {}", .{ total_seconds, err });
            };
        },

        else => panic("invalid time value: {f}", .{v.*}),
    }
}

test "gtfs.conversion_to_gtfs.time" {
    var v = ColumnValue.int(12 * 3600 + 15 * 60 + 30);
    time(&v);
    try std.testing.expectEqualStrings("12:15:30", try v.ensureString());

    v = ColumnValue.null_();
    try std.testing.expectEqualStrings("", try v.ensureString());
}

/// maybeWithZeroUnknown converts an Impuls tri-state (NULL, 0/false, 1/true) into
/// a GTFS tri-state enum (0, 1, 2).
pub fn maybeWithZeroUnknown(v: *ColumnValue) void {
    switch (v.*) {
        .Int => |i| v.* = ColumnValue.int(if (i != 0) 1 else 2),
        .Null => v.* = ColumnValue.int(0),
        else => {},
    }
}

test "gtfs.conversion_to_gtfs.maybeWithZeroUnknown" {
    var v = ColumnValue.null_();
    maybeWithZeroUnknown(&v);
    try std.testing.expectEqual(@as(i64, 0), v.Int);

    v = ColumnValue.int(0);
    maybeWithZeroUnknown(&v);
    try std.testing.expectEqual(@as(i64, 2), v.Int);

    v = ColumnValue.int(1);
    maybeWithZeroUnknown(&v);
    try std.testing.expectEqual(@as(i64, 1), v.Int);
}
