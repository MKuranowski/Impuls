// © Copyright 2022-2024 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const std = @import("std");
const assert = std.debug.assert;
const io = std.io;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

/// Parser parses [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180#section-2) CSV files.
///
/// The parser deviates from the standard in the following ways:
/// 1. Fields (TEXTDATA) may contain any bytes, as long as they are not COMMA, DQOUTE or CRLF.
/// 2. Delimiters (COMMA) may be set to any other octet.
/// 3. Quoting character (DQUOTE) may be set to any other octet; or set to null - in which case
///    no special processing of quotes is applied.
/// 4. Record terminator (CRLF) may be set to any other octet; or the CR-LF sequence.
///    If set to the latter, Parser will also recognize sole LF octets as record terminators.
/// 5. Fields can also be a concatenation of `escaped` and `non-escaped` data. This is done for
///    handling simplicit. Fields like `"foo"bar` are not treated as an error, but are parsed
///    as `"foobar"`.
/// 6. DQUOTE present in a `non-escaped` field is not treated as an error, rather the DQUOTE octet
///    is appended to the field. Encodings `"Foo ""Bar"" Baz"` and `Foo "Bar" Baz` are equivalent
///    and parse the same as the string literal `"Foo \"Bar\" Baz"`.
///
/// `ReaderType` can by any type with a `fn readByte(self) Error || error{EndOfStream} ! u8`
/// method. For performance reasons, it is recommended to use a io.BufferedReader().reader().
pub fn Reader(comptime ReaderType: type) type {
    return struct {
        const Self = @This();

        r: ReaderType,
        delimiter: u8 = ',',
        quote: ?u8 = '"',
        terminator: Terminator = Terminator{ .crlf = {} },

        line_no: u32 = 1,
        state: State = .before_record,

        pub fn init(r: ReaderType) Self {
            return Self{ .r = r };
        }

        pub fn next(self: *Self, record: *Record) !bool {
            record.line_no = self.line_no;
            record.clear();

            while (true) {
                const b = self.getByte() catch |err| {
                    if (err == error.EndOfStream) {
                        if (self.state != .before_record) {
                            self.state = .before_record;
                            try record.pushField();
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return err;
                    }
                };

                if (self.state == .after_unquoted_cr) {
                    if (b == '\n') {
                        try record.pushField();
                        self.state = .before_record;
                        return true;
                    } else {
                        try record.pushByte('\r');
                        self.state = .in_field;
                        // fallthrough
                    }
                }

                if (self.state == .before_field or self.state == .before_record) {
                    if (b == self.quote) {
                        self.state = .in_quoted_field;
                        continue;
                    } else {
                        self.state = .in_field;
                        // fallthrough
                    }
                }

                if (self.state == .quote_in_quoted) {
                    if (b == self.quote) {
                        try record.pushByte(b);
                        self.state = .in_quoted_field;
                        continue;
                    } else {
                        self.state = .in_field;
                        // fallthrough
                    }
                }

                if (self.state == .in_field) {
                    if (b == self.delimiter) {
                        try record.pushField();
                        self.state = .before_field;
                        continue;
                    }

                    switch (self.terminator.match(b)) {
                        .all => {
                            try record.pushField();
                            self.state = .before_record;
                            return true;
                        },
                        .cr => {
                            self.state = .after_unquoted_cr;
                            continue;
                        },
                        .none => {
                            try record.pushByte(b);
                            continue;
                        },
                    }
                }

                if (self.state == .in_quoted_field) {
                    if (b == self.quote) {
                        self.state = .quote_in_quoted;
                        continue;
                    } else {
                        try record.pushByte(b);
                        continue;
                    }
                }
            }
        }

        fn getByte(self: *Self) !u8 {
            const b = try self.r.readByte();
            if (b == '\n') self.line_no += 1;
            return b;
        }
    };
}

/// reader returns an initialized Reader over a given io.Reader instance.
/// See Reader documentation for details.
pub fn reader(r: anytype) Reader(@TypeOf(r)) {
    return Reader(@TypeOf(r)).init(r);
}

/// Writers writes [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180#section-2) CSV files.
///
/// The writer deviates from the standard in a single way:
/// 1. Field (TEXTDATA) may contain any bytes, as long as they are not COMMA, DQOUTE or CRLF.
///
/// It's not possible to deviate from the standard (by e.g. customizing the field terminator)
/// for two reasons:
/// 1. religious: a standard is a standard, whether you like it or not. The C in CSV stands
///     for comma, not semicolon or pipe. Also, "be conservative in what you do, be liberal in what
///     you accept from others" - [RFC 793](https://www.rfc-editor.org/rfc/rfc793#section-2.10).
/// 2. practical: to check whether a field needs to be escaped a simple call to
///     `std.mem.indexOfAny(field, ",\"\r\n")` is used. Allowing customizable field or record
///     terminators would require preparing the illegal-characters string at runtime.
pub fn Writer(comptime WriterType: type) type {
    return struct {
        const Self = @This();

        w: WriterType,
        needsComma: bool = false,

        fn init(w: WriterType) Self {
            return Self{ .w = w };
        }

        /// writeRecord writes a CSV record to the underlying writer.
        ///
        /// _record_ can be either a slice, pointer-to-many or a tuple of []const u8
        /// (or anything which can automatically be coerced to []const u8).
        ///
        /// It's forbidden to mix writeRecord and writeField calls to write a single record,
        /// if mixing both functions, a writeRecord can't follow a call to writeField -
        /// terminateRecord must be called first.
        pub fn writeRecord(self: *Self, record: anytype) !void {
            assert(!self.needsComma); // writeRecord called without terminating previous row.

            switch (@typeInfo(@TypeOf(record))) {
                // Slice of fields
                .Pointer => |ptr| {
                    if (ptr.size == .Slice or ptr.size == .Many) {
                        for (record) |field| try self.writeField(field);
                        try self.terminateRecord();
                        return;
                    }
                },

                // Tuple of fields
                .Struct => |str| {
                    if (str.is_tuple) {
                        inline for (record) |field| try self.writeField(field);
                        try self.terminateRecord();
                        return;
                    }
                },

                else => {},
            }

            @compileError(@typeName(@TypeOf(record)) ++ " can't be interpreted as a CSV record");
        }

        /// writeField writes a CSV field to the underlying writer.
        ///
        /// The caller must also call terminateRecord() once all fields
        /// of the record have been written.
        pub fn writeField(self: *Self, field: []const u8) !void {
            if (self.needsComma) try self.w.writeByte(',');
            self.needsComma = true;

            if (Self.needsEscaping(field)) {
                try self.w.writeByte('"');
                for (field) |octet| {
                    if (octet == '"') try self.w.writeByte(octet);
                    try self.w.writeByte(octet);
                }
                try self.w.writeByte('"');
            } else {
                try self.w.writeAll(field);
            }
        }

        /// terminateRecord writes the record terminator to the underlying writer.
        pub fn terminateRecord(self: *Self) !void {
            self.needsComma = false;
            try self.w.writeAll("\r\n");
        }

        fn needsEscaping(field: []const u8) bool {
            return mem.indexOfAny(u8, field, ",\"\r\n") != null;
        }
    };
}

pub fn writer(w: anytype) Writer(@TypeOf(w)) {
    return Writer(@TypeOf(w)).init(w);
}

/// Record represents a single record from a CSV file.
///
/// Record internally consits of an ArrayList(ArrayList(u8)). Not all elements hold
/// valid fields. Elements which do hold a read field are called "complete".
pub const Record = struct {
    allocator: Allocator,

    /// Line number of the record in the source file. If records spans multiple lines,
    /// it's the first line of the record.
    ///
    /// Line numbers are determined by the number of '\n' octets encountered in the stream,
    /// **not** the number of terminators.
    line_no: u32 = 0,

    /// Array of buffers for fields. Length has to be >= self.complete_fields.
    /// Extra elements preserve allocated buffers for next fields, to avoid reallocations.
    field_buffers: ArrayListUnmanaged(ArrayListUnmanaged(u8)) = .{},

    /// Number of complete fields in `field_buffers`.
    /// `field_buffers[0..complete_fields]` represents completely parsed fields.
    /// `field_buffers[comeplete_fields]`, if present, contains a field being built.
    complete_fields: usize = 0,

    pub fn init(allocator: Allocator) Record {
        return Record{ .allocator = allocator };
    }

    pub fn deinit(self: *Record) void {
        for (self.field_buffers.items) |*field_buffer| {
            field_buffer.deinit(self.allocator);
        }
        self.field_buffers.deinit(self.allocator);
    }

    /// line returns the number of complete fields in this record
    pub inline fn len(self: Record) usize {
        assert(self.field_buffers.items.len >= self.complete_fields); // invariant for complete fields
        return self.complete_fields;
    }

    /// get returns the ith complete field, asserting that it's a complete field.
    pub inline fn get(self: Record, i: usize) []u8 {
        assert(i < self.complete_fields); // check if complete field is accessed
        assert(self.field_buffers.items.len >= self.complete_fields); // invariant for complete fields
        return self.field_buffers.items[i].items;
    }

    /// get returns the ith complete field, or null if no valid field exists at the provided index.
    pub inline fn getSafe(self: Record, i: usize) ?[]u8 {
        assert(self.field_buffers.items.len >= self.complete_fields); // invariant for complete fields
        return if (i < self.comeplete_fields) self.field_buffers.items[i].items else null;
    }

    /// slice returns a slice over arrays holding the complete fields.
    pub inline fn slice(self: Record) []ArrayListUnmanaged(u8) {
        return self.field_buffers.items[0..self.complete_fields];
    }

    /// clear clears the record, setting each field_buffer to zero length (without deallocation),
    /// and setting the number of complete fields to zero.
    pub fn clear(self: *Record) void {
        self.complete_fields = 0;
        for (self.field_buffers.items) |*field_buffer| {
            field_buffer.clearRetainingCapacity();
        }
    }

    /// pushField marks field-being-built as complete. If no field is being built, a ""
    /// is added as a complete field.
    pub fn pushField(self: *Record) !void {
        try self.ensureIncompleteField();
        self.complete_fields += 1;
    }

    /// Adds a byte to the field-being-built, allocating that field if necessary.
    pub fn pushByte(self: *Record, b: u8) !void {
        try self.ensureIncompleteField();
        try self.field_buffers.items[self.complete_fields].append(self.allocator, b);
    }

    /// Adds a byte to the field-being-built, allocating that field if necessary.
    pub fn pushBytes(self: *Record, b: []const u8) !void {
        try self.ensureIncompleteField();
        try self.field_buffers.items[self.complete_fields].appendSlice(self.allocator, b);
    }

    /// Ensures the field-being-build (field_buffers[complete_fields]) exists.
    fn ensureIncompleteField(self: *Record) !void {
        assert(self.field_buffers.items.len >= self.complete_fields); // invariant for complete fields

        if (self.field_buffers.items.len == self.complete_fields) {
            try self.field_buffers.append(self.allocator, .{});
        }
    }
};

/// Terminator represents a record delimiter - either a specific octet, or the CR LF sequence.
pub const Terminator = union(enum) {
    octet: u8,
    crlf,

    /// match checks if the provided byte is a terminator.
    fn match(self: Terminator, b: u8) TerminatorMatch {
        switch (self) {
            .octet => |terminator| return if (b == terminator) .all else .none,
            .crlf => return switch (b) {
                '\r' => .cr,
                '\n' => .all,
                else => .none,
            },
        }
    }
};

const TerminatorMatch = enum {
    none,
    cr,
    all,
};

const State = enum {
    before_record,
    before_field,
    in_field,
    in_quoted_field,
    quote_in_quoted,
    after_unquoted_cr,
};

test "csv.reading.basic" {
    const data = "pi,3.1416\r\nsqrt2,1.4142\r\nphi,1.618\r\ne,2.7183\r\n";
    var stream = io.fixedBufferStream(data);
    var r = reader(stream.reader());

    var record = Record.init(std.testing.allocator);
    defer record.deinit();

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 1), record.line_no);
    try std.testing.expectEqual(@as(usize, 2), record.len());
    try std.testing.expectEqualStrings("pi", record.get(0));
    try std.testing.expectEqualStrings("3.1416", record.get(1));

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 2), record.line_no);
    try std.testing.expectEqual(@as(usize, 2), record.len());
    try std.testing.expectEqualStrings("sqrt2", record.get(0));
    try std.testing.expectEqualStrings("1.4142", record.get(1));

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 3), record.line_no);
    try std.testing.expectEqual(@as(usize, 2), record.len());
    try std.testing.expectEqualStrings("phi", record.get(0));
    try std.testing.expectEqualStrings("1.618", record.get(1));

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 4), record.line_no);
    try std.testing.expectEqual(@as(usize, 2), record.len());
    try std.testing.expectEqualStrings("e", record.get(0));
    try std.testing.expectEqualStrings("2.7183", record.get(1));

    try std.testing.expect(!try r.next(&record));
}

test "csv.reading.with_quoted_fields" {
    const data =
        \\"hello","is it ""me""","you're
        \\looking for"
        \\"it's another
        \\record",with a newline inside,"but no ""trailing"" "one!
    ;

    var stream = io.fixedBufferStream(data);
    var r = reader(stream.reader());

    var record = Record.init(std.testing.allocator);
    defer record.deinit();

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 1), record.line_no);
    try std.testing.expectEqual(@as(usize, 3), record.len());
    try std.testing.expectEqualStrings("hello", record.get(0));
    try std.testing.expectEqualStrings("is it \"me\"", record.get(1));
    try std.testing.expectEqualStrings("you're\nlooking for", record.get(2));

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 3), record.line_no);
    try std.testing.expectEqual(@as(usize, 3), record.len());
    try std.testing.expectEqualStrings("it's another\nrecord", record.get(0));
    try std.testing.expectEqualStrings("with a newline inside", record.get(1));
    try std.testing.expectEqualStrings("but no \"trailing\" one!", record.get(2));

    try std.testing.expect(!try r.next(&record));
}

test "csv.reading_with_custom_dialect" {
    const data = "foo|bar#\"no quote handling|\"so this is another field#";
    var stream = io.fixedBufferStream(data);
    var r = reader(stream.reader());
    r.delimiter = '|';
    r.quote = null;
    r.terminator = .{ .octet = '#' };

    var record = Record.init(std.testing.allocator);
    defer record.deinit();

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 1), record.line_no);
    try std.testing.expectEqual(@as(usize, 2), record.len());
    try std.testing.expectEqualStrings("foo", record.get(0));
    try std.testing.expectEqualStrings("bar", record.get(1));

    try std.testing.expect(try r.next(&record));
    try std.testing.expectEqual(@as(u32, 1), record.line_no);
    try std.testing.expectEqual(@as(usize, 2), record.len());
    try std.testing.expectEqualStrings("\"no quote handling", record.get(0));
    try std.testing.expectEqualStrings("\"so this is another field", record.get(1));
}

test "csv.writing" {
    var data = std.ArrayList(u8).init(std.testing.allocator);
    defer data.deinit();

    var w = writer(data.writer());
    try w.writeRecord(.{ "foo", "bar", "baz" });

    try w.writeField("this field needs to be \"escaped\"");
    try w.writeField("and, this one\ntoo?");
    try w.writeField("but this one - 'no'");
    try w.terminateRecord();

    try std.testing.expectEqualStrings("foo,bar,baz\r\n\"this field needs to be \"\"escaped\"\"\",\"and, this one\ntoo?\",but this one - 'no'\r\n", data.items);
}
