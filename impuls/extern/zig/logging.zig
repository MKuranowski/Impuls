const std = @import("std");

pub const Level = struct {
    pub const NOTSET = 0;
    pub const DEBUG = 10;
    pub const INFO = 20;
    pub const WARNING = 30;
    pub const ERROR = 40;
    pub const CRITICAL = 50;

    pub fn describe(level: c_int) []const u8 {
        if (level >= 50) {
            return "CRITICAL";
        } else if (level >= 40) {
            return "ERROR";
        } else if (level >= 30) {
            return "WARNING";
        } else if (level >= 20) {
            return "INFO";
        } else if (level >= 10) {
            return "DEBUG";
        }
        return "not set";
    }
};

pub const Handler = *const fn (c_int, [*:0]const u8) callconv(.C) void;

pub const Logger = struct {
    handler: Handler,

    pub fn log(self: Logger, level: c_int, comptime fmt: [:0]const u8, args: anytype) void {
        if (args.len == 0) {
            self.handler(level, fmt);
        } else {
            var buf: [8192]u8 = undefined;
            var msg = std.fmt.bufPrintZ(&buf, fmt, args) catch |e| blk: {
                switch (e) {
                    error.NoSpaceLeft => {
                        buf[buf.len - 1] = 0;
                        break :blk buf[0 .. buf.len - 1 :0];
                    },
                }
            };
            self.handler(level, msg);
        }
    }

    pub inline fn debug(self: Logger, comptime fmt: [:0]const u8, args: anytype) void {
        self.log(Level.DEBUG, fmt, args);
    }

    pub inline fn info(self: Logger, comptime fmt: [:0]const u8, args: anytype) void {
        self.log(Level.INFO, fmt, args);
    }

    pub inline fn warn(self: Logger, comptime fmt: [:0]const u8, args: anytype) void {
        self.log(Level.WARNING, fmt, args);
    }

    pub inline fn err(self: Logger, comptime fmt: [:0]const u8, args: anytype) void {
        self.log(Level.ERROR, fmt, args);
    }

    pub inline fn critical(self: Logger, comptime fmt: [:0]const u8, args: anytype) void {
        self.log(Level.CRITICAL, fmt, args);
    }
};

pub fn stderr_handler(level: c_int, msg: [*:0]const u8) callconv(.C) void {
    const now_sec = std.time.epoch.EpochSeconds{ .secs = @intCast(std.time.timestamp()) };
    const time = now_sec.getDaySeconds();
    const date = now_sec.getEpochDay().calculateYearDay();
    const month_day = date.calculateMonthDay();

    std.debug.print("{d:04}-{d:02}-{d:02}T{d:02}:{d:02}:{d:02} {s} {s}\n", .{
        date.year,
        month_day.month.numeric(),
        month_day.day_index,
        time.getHoursIntoDay(),
        time.getMinutesIntoHour(),
        time.getSecondsIntoMinute(),
        Level.describe(level),
        msg,
    });
}

pub const StderrLogger = Logger{ .handler = stderr_handler };
