const c = @import("./conversion.zig");
const std = @import("std");

const BoundedString = c.BoundedString;
const ColumnValue = c.ColumnValue;
const fmt = std.fmt;
const InvalidValue = c.InvalidValue;
const InvalidValueT = c.InvalidValueT;

pub fn asIs(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    return ColumnValue.borrowed(s);
}

pub fn optional(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.null_() else ColumnValue.borrowed(s);
}

pub fn int(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    const i = fmt.parseInt(i64, s, 10) catch return InvalidValue;
    return ColumnValue.int(i);
}

pub fn optionalInt(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.null_() else int(s, line_no);
}

pub fn intFallbackZero(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.int(0) else int(s, line_no);
}

pub fn float(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    const f = fmt.parseFloat(f64, s) catch return InvalidValue;
    return ColumnValue.float(f);
}

pub fn optionalFloat(s: []const u8, line_no: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) ColumnValue.null_() else float(s, line_no);
}

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

pub fn maybeWithZeroFalse(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    if (s.len == 0) return ColumnValue.null_();
    if (s.len > 1) return InvalidValue;
    switch (s[0]) {
        '0' => return ColumnValue.int(0),
        '1' => return ColumnValue.int(1),
        else => return InvalidValue,
    }
}

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

pub fn optionalDate(s: []const u8, i: u32) InvalidValueT!ColumnValue {
    return if (s.len == 0) return ColumnValue.null_() else date(s, i);
}

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

pub fn agencyId(s: []const u8, _: u32) InvalidValueT!ColumnValue {
    return ColumnValue.borrowed(if (s.len == 0) "(missing)" else s);
}

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

pub fn feedInfoId(_: []const u8, _: u32) InvalidValueT!ColumnValue {
    return ColumnValue.int(0);
}
