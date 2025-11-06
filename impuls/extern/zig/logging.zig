// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const std = @import("std");

pub const Handler = *const fn (c_int, [*:0]const u8) callconv(.c) void;
pub var custom_handler: ?Handler = null;

pub fn logWithCustomHandler(
    comptime message_level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    if (!std.log.logEnabled(message_level, scope)) return;

    if (custom_handler) |handler| {
        var buf: [8192]u8 = undefined;
        const msg: [:0]u8 = std.fmt.bufPrintZ(&buf, format, args) catch |err| blk: {
            switch (err) {
                error.NoSpaceLeft => {
                    buf[buf.len - 1] = 0;
                    break :blk buf[0 .. buf.len - 1 :0];
                },
            }
        };
        handler(convertLevelToPython(message_level), msg);
    } else {
        std.log.defaultLog(message_level, scope, format, args);
    }
}

fn convertLevelToPython(level: std.log.Level) c_int {
    return switch (level) {
        .debug => 10,
        .info => 20,
        .warn => 30,
        .err => 40,
    };
}
