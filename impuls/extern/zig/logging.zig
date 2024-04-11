const std = @import("std");

const Level = struct {
    const NOTSET = 0;
    const DEBUG = 10;
    const INFO = 20;
    const WARNING = 30;
    const ERROR = 40;
    const CRITICAL = 50;
};

pub const Handler = *const fn (c_int, [*:0]const u8) callconv(.C) void;

pub const Logger = struct {
    handler: Handler,

    pub fn log(self: Logger, level: c_int, fmt: []const u8, args: anytype) void {
        if (args.len == 0) {
            self.handler(level, fmt);
        } else {
            var buf: [8192]u8 = undefined;
            var msg = std.fmt.bufPrintZ(&buf, fmt, args) catch |e| {
                if (e == error.NoSpaceLeft) {
                    buf[buf.len - 1] = 0;
                    buf[0 .. buf.len - 1 :0];
                } else {
                    self.handler(Level.ERROR, "Failed to format log message :^( - " ++ @errorName(e));
                    return;
                }
            };
            self.handler(level, msg);
        }
    }

    pub inline fn debug(self: Logger, fmt: []const u8, args: anytype) void {
        self.log(Level.DEBUG, fmt, args);
    }

    pub inline fn info(self: Logger, fmt: []const u8, args: anytype) void {
        self.log(Level.INFO, fmt, args);
    }

    pub inline fn warn(self: Logger, fmt: []const u8, args: anytype) void {
        self.log(Level.WARNING, fmt, args);
    }

    pub inline fn err(self: Logger, fmt: []const u8, args: anytype) void {
        self.log(Level.ERROR, fmt, args);
    }

    pub inline fn critical(self: Logger, fmt: []const u8, args: anytype) void {
        self.log(Level.CRITICAL, fmt, args);
    }
};
