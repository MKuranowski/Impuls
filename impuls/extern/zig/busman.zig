pub fn load(
    db_path: [*:0]const u8,
    mdb_path: [*:0]const u8,
    agency_id: [*:0]const u8,
    ignore_route_id: bool,
    ignore_stop_id: bool,
) !void {
    _ = db_path;
    _ = mdb_path;
    _ = agency_id;
    _ = ignore_route_id;
    _ = ignore_stop_id;

    return error.NotImplemented;
}
