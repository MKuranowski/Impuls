const c = @import("./conversion.zig");
const std = @import("std");

const assert = std.debug.assert;
const BoundedString = c.BoundedString;
const ColumnValue = c.ColumnValue;
const panic = std.debug.panic;

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

        else => panic("invalid date value: {}", .{v.*}),
    }
}

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

        else => panic("invalid time value: {}", .{v.*}),
    }
}

pub fn maybeWithZeroUnknown(v: *ColumnValue) void {
    switch (v.*) {
        .Int => |i| v.* = ColumnValue.int(if (i != 0) 1 else 2),
        .Null => v.* = ColumnValue.int(0),
        else => {},
    }
}
