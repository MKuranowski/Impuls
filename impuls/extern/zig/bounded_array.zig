// © Copyright 2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

pub fn BoundedArray(comptime T: type, comptime cap: usize) type {
    return struct {
        const Self = @This();

        buffer: [cap]T = undefined,
        len: usize = 0,

        pub fn init(len: usize) error{Overflow}!Self {
            if (len > cap) return error.Overflow;
            return Self{ .len = len };
        }

        pub fn slice(self: *Self) []T {
            return self.buffer[0..self.len];
        }

        pub fn constSlice(self: *const Self) []const T {
            return self.buffer[0..self.len];
        }

        pub fn append(self: *Self, item: T) error{Overflow}!void {
            const new_len = self.len + 1;
            if (new_len > cap) return error.Overflow;

            self.buffer[self.len] = item;
            self.len = new_len;
        }

        pub fn appendSlice(self: *Self, s: []const T) error{Overflow}!void {
            const new_len = self.len + s.len;
            if (new_len > cap) return error.Overflow;

            @memcpy(self.buffer[self.len..new_len], s);
            self.len = new_len;
        }
    };
}
