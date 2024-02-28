comptime {
    // C guarantees that sizeof(char) is 1 byte,
    // but doesn't guarante that one byte is exactly 8 bits.
    // Platforms with non-8-bit-bytes exist, but are extremely uncommon.
    // Since this module treats c_char and u8 interchangebly, crash
    // if those types have different sizes.
    if (@typeInfo(c_char).Int.bits != @typeInfo(u8).Int.bits)
        @compileError("u8 and c_char have different widths. This module expectes those types to be interchangable.");
}

/// Non-null pointer to a null-terminated C byte string, aka `char const*`.
const c_char_p = [*:0]const u8;

/// Non-null pointer to a null-terminated vector of c_strings, aka `char const* const*`.
const c_char_p_p = [*:null]const ?c_char_p;

pub const Headers = extern struct {
    agency: ?c_char_p_p = null,
    attributions: ?c_char_p_p = null,
    calendar: ?c_char_p_p = null,
    calendar_dates: ?c_char_p_p = null,
    feed_info: ?c_char_p_p = null,
    routes: ?c_char_p_p = null,
    stops: ?c_char_p_p = null,
    shapes: ?c_char_p_p = null,
    trips: ?c_char_p_p = null,
    stop_times: ?c_char_p_p = null,
    frequencies: ?c_char_p_p = null,
    transfers: ?c_char_p_p = null,
    fare_attributes: ?c_char_p_p = null,
    fare_rules: ?c_char_p_p = null,
};

pub fn load(db_path: [*:0]const u8, gtfs_dir_path: [*:0]const u8) !void {
    _ = db_path;
    _ = gtfs_dir_path;

    return error.NotImplemented;
}

pub fn save(
    db_path: [*:0]const u8,
    gtfs_dir_path: [*:0]const u8,
    headers: *Headers,
    emit_empty_calendars: bool,
) !void {
    _ = db_path;
    _ = gtfs_dir_path;
    _ = headers;
    _ = emit_empty_calendars;

    return error.NotImplemented;
}
