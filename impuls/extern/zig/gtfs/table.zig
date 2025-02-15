// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const c = @import("./conversion.zig");
const from_gtfs = @import("./conversion_from_gtfs.zig");
const std = @import("std");
const to_gtfs = @import("./conversion_to_gtfs.zig");

const StaticStringMap = std.StaticStringMap;
const FnFromGtfs = c.FnFromGtfs;
const FnToGtfs = c.FnToGtfs;
const mem = std.mem;

/// Table contains data necessary for mapping between GTFS and Impuls (SQL) tables.
pub const Table = struct {
    /// gtfs_name contains the GTFS table name, with the .txt extension
    gtfs_name: [:0]const u8,

    /// sql_name contains the Impuls (SQL) table name
    sql_name: [:0]const u8,

    /// required denotes if this table must be present in order to consider a GTFS file valid.
    required: bool = false,

    /// columns contains specifics on column mapping between the schemas
    columns: []const Column,

    /// parent_implication, if present, describes the existence of parent objects in GTFS
    parent_implication: ?ParentImplication = null,

    /// has_extra_fields_json is set to true if there is the SQL table has an additional
    /// "extra_fields_json" TEXT column with a JSON object mapping extra GTFS fields to its
    /// string values.
    ///
    /// The "extra_fields_json" column is never present in the `columns`, `columnNames` and
    /// `placeholders`.
    has_extra_fields_json: bool = false,

    /// order_clause contains an optional " ORDER BY ..." (with a leading space) to enforce
    /// a defined ordering when serializing a table to CSV.
    order_clause: []const u8 = "",

    /// columnNames returns a "column_a, column_b, column_c" string with the SQL column names
    /// of the table.
    pub fn columnNames(comptime self: Table) []const u8 {
        comptime var s: []const u8 = "";
        comptime var sep: []const u8 = "";
        inline for (self.columns) |column| {
            s = s ++ sep ++ column.name;
            sep = ", ";
        }
        return s;
    }

    /// placeholders returns a "?, ?, ?" string with SQL placeholders, as many as there
    /// are SQL columns.
    pub fn placeholders(comptime self: Table) []const u8 {
        comptime var s: []const u8 = "";
        comptime var chunk: []const u8 = "?";
        inline for (self.columns) |_| {
            s = s ++ chunk;
            chunk = ", ?";
        }
        return s;
    }

    /// gtfsColumnNamesToIndices creates a std.StaticStringMap mapping GTFS column names
    /// to indices into Table.columns.
    pub fn gtfsColumnNamesToIndices(comptime self: Table) StaticStringMap(usize) {
        @setEvalBranchQuota(10_000);
        comptime var kvs: [self.columns.len]struct { []const u8, usize } = undefined;
        inline for (self.columns, 0..) |col, i| {
            kvs[i] = .{ col.gtfsName(), i };
        }
        return StaticStringMap(usize).initComptime(kvs);
    }

    pub fn gtfsColumnNameToIndex(self: Table, column_name: []const u8) ?usize {
        for (self.columns, 0..) |column, index| {
            if (mem.eql(u8, column_name, column.gtfsName())) {
                return index;
            }
        }
        return null;
    }

    /// gtfsNameWithoutExtension returns the GTFS name of the table without the ".txt" extension
    pub fn gtfsNameWithoutExtension(comptime self: Table) []const u8 {
        comptime {
            if (!mem.endsWith(u8, self.gtfs_name, ".txt")) {
                @compileError("gtfs_name of a Table doesn't end with .txt: " ++ self.gtfs_name);
            }
        }
        return self.gtfs_name[0 .. self.gtfs_name.len - 4];
    }
};

/// ParentImplication describes the existence of parent objects in GTFS for a particular table.
///
/// For example, a calendar exception from GTFS's calendar_dates table implies
/// the existence of a parent calendar, even if it wasn't defined in the calendar table.
/// Impuls doesn't allow for implicit objects, and an extra INSERT may be necessary to
/// ensure foreign key references remain valid.
pub const ParentImplication = struct {
    /// sql_table names the SQL table name in which implied entities need to be the create.
    sql_table: []const u8,

    /// sql_key names the primary key of the sql_table.
    sql_key: []const u8,

    /// gtfs_key names the foreign key column in the GTFS table.
    gtfs_key: []const u8,
};

/// Column contains necessary information for mapping between GTFS and Impuls columns.
pub const Column = struct {
    /// name contains the Impuls (SQL) column name.
    name: [:0]const u8,

    /// gtfs_name contains the GTFS column name, only if that is different than the `name`.
    /// Use the `getName()` getter to a non-optional GTFS column name.
    gtfs_name: ?[:0]const u8 = null,

    /// convert_to_sql takes the raw GTFS column value (or "" if the column is missing)
    /// and a line_number to produce an equivalent Impuls (SQL) value.
    from_gtfs: FnFromGtfs = from_gtfs.asIs,

    /// convert_to_gtfs, if present, takes the raw SQL column value, and adjusts the value of
    /// the column, so that ColumnValue.ensureString() will be a valid GTFS value.
    to_gtfs: ?FnToGtfs = null,

    /// gtfsName returns the GTFS name of the column.
    pub inline fn gtfsName(self: Column) [:0]const u8 {
        return if (self.gtfs_name) |gtfs_name| gtfs_name else self.name;
    }
};

/// Column describes how to map GTFS and SQL columns.
pub const ColumnMapping = union(enum) {
    /// standard represents a normal Column, present in the `Table.columns`
    /// under the provided index. Custom conversions may apply.
    standard: usize,

    /// extra represents an extra Column, present in `extra_fields_json`
    /// under the provided key. Used only if `Table.has_extra_fields_json` is set.
    extra: []const u8,

    /// none represents a Column which can't be found in an SQL record.
    /// Used only if `Table.has_extra_fields_json` is not set.
    none,
};

/// tables lists all known Table mappings between GTFS and Impuls models.
pub const tables = [_]Table{
    Table{
        .gtfs_name = "agency.txt",
        .sql_name = "agencies",
        .required = true,
        .columns = &[_]Column{
            Column{ .name = "agency_id", .from_gtfs = from_gtfs.agencyId },
            Column{ .name = "name", .gtfs_name = "agency_name" },
            Column{ .name = "url", .gtfs_name = "agency_url" },
            Column{ .name = "timezone", .gtfs_name = "agency_timezone" },
            Column{ .name = "lang", .gtfs_name = "agency_lang" },
            Column{ .name = "phone", .gtfs_name = "agency_phone" },
            Column{ .name = "fare_url", .gtfs_name = "agency_fare_url" },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY agency_id",
    },
    Table{
        .gtfs_name = "attributions.txt",
        .sql_name = "attributions",
        .columns = &[_]Column{
            Column{ .name = "attribution_id", .from_gtfs = from_gtfs.attributionId },
            Column{ .name = "organization_name" },
            Column{ .name = "is_producer", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "is_operator", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "is_authority", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "is_data_source", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "url", .gtfs_name = "attribution_url" },
            Column{ .name = "email", .gtfs_name = "attribution_email" },
            Column{ .name = "phone", .gtfs_name = "attribution_phone" },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY attribution_id",
    },
    Table{
        .gtfs_name = "calendar.txt",
        .sql_name = "calendars",
        .columns = &[_]Column{
            // XXX: The order of columns must be kept in sync with isCalendarEmpty
            Column{ .name = "calendar_id", .gtfs_name = "service_id" },
            Column{ .name = "monday", .from_gtfs = from_gtfs.int },
            Column{ .name = "tuesday", .from_gtfs = from_gtfs.int },
            Column{ .name = "wednesday", .from_gtfs = from_gtfs.int },
            Column{ .name = "thursday", .from_gtfs = from_gtfs.int },
            Column{ .name = "friday", .from_gtfs = from_gtfs.int },
            Column{ .name = "saturday", .from_gtfs = from_gtfs.int },
            Column{ .name = "sunday", .from_gtfs = from_gtfs.int },
            Column{ .name = "start_date", .from_gtfs = from_gtfs.date, .to_gtfs = to_gtfs.date },
            Column{ .name = "end_date", .from_gtfs = from_gtfs.date, .to_gtfs = to_gtfs.date },
            Column{ .name = "desc", .gtfs_name = "service_desc" },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY calendar_id",
    },
    Table{
        .gtfs_name = "calendar_dates.txt",
        .sql_name = "calendar_exceptions",
        .columns = &[_]Column{
            Column{ .name = "calendar_id", .gtfs_name = "service_id" },
            Column{ .name = "date", .from_gtfs = from_gtfs.date, .to_gtfs = to_gtfs.date },
            Column{ .name = "exception_type", .from_gtfs = from_gtfs.int },
        },
        .parent_implication = ParentImplication{
            .sql_table = "calendars",
            .sql_key = "calendar_id",
            .gtfs_key = "service_id",
        },
        .order_clause = " ORDER BY calendar_id, date",
    },
    Table{
        .gtfs_name = "feed_info.txt",
        .sql_name = "feed_info",
        .columns = &[_]Column{
            Column{
                .name = "feed_info_id",
                .gtfs_name = "",
                .from_gtfs = from_gtfs.feedInfoId,
            },
            Column{ .name = "publisher_name", .gtfs_name = "feed_publisher_name" },
            Column{ .name = "publisher_url", .gtfs_name = "feed_publisher_url" },
            Column{ .name = "lang", .gtfs_name = "feed_lang" },
            Column{ .name = "version", .gtfs_name = "feed_version" },
            Column{ .name = "contact_email", .gtfs_name = "feed_contact_email" },
            Column{ .name = "contact_url", .gtfs_name = "feed_contact_url" },
            Column{
                .name = "start_date",
                .gtfs_name = "feed_start_date",
                .from_gtfs = from_gtfs.optionalDate,
                .to_gtfs = to_gtfs.date,
            },
            Column{
                .name = "end_date",
                .gtfs_name = "feed_end_date",
                .from_gtfs = from_gtfs.optionalDate,
                .to_gtfs = to_gtfs.date,
            },
        },
        .has_extra_fields_json = true,
    },
    Table{
        .gtfs_name = "routes.txt",
        .sql_name = "routes",
        .required = true,
        .columns = &[_]Column{
            Column{ .name = "route_id" },
            Column{ .name = "agency_id", .from_gtfs = from_gtfs.agencyId },
            Column{ .name = "short_name", .gtfs_name = "route_short_name" },
            Column{ .name = "long_name", .gtfs_name = "route_long_name" },
            Column{ .name = "type", .gtfs_name = "route_type", .from_gtfs = from_gtfs.routeType },
            Column{ .name = "color", .gtfs_name = "route_color" },
            Column{ .name = "text_color", .gtfs_name = "route_text_color" },
            Column{ .name = "sort_order", .gtfs_name = "route_sort_order", .from_gtfs = from_gtfs.optionalInt },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY route_id",
    },
    Table{
        .gtfs_name = "stops.txt",
        .sql_name = "stops",
        .required = true,
        .columns = &[_]Column{
            Column{ .name = "stop_id" },
            Column{ .name = "name", .gtfs_name = "stop_name" },
            Column{ .name = "lat", .gtfs_name = "stop_lat", .from_gtfs = from_gtfs.float },
            Column{ .name = "lon", .gtfs_name = "stop_lon", .from_gtfs = from_gtfs.float },
            Column{ .name = "code", .gtfs_name = "stop_code" },
            Column{ .name = "zone_id", .gtfs_name = "zone_id" },
            Column{ .name = "location_type", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "parent_station", .from_gtfs = from_gtfs.optional },
            Column{
                .name = "wheelchair_boarding",
                .from_gtfs = from_gtfs.maybeWithZeroUnknown,
                .to_gtfs = to_gtfs.maybeWithZeroUnknown,
            },
            Column{ .name = "platform_code" },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY stop_id",
    },
    Table{
        .gtfs_name = "fare_attributes.txt",
        .sql_name = "fare_attributes",
        .columns = &[_]Column{
            Column{ .name = "fare_id" },
            Column{ .name = "price", .from_gtfs = from_gtfs.float },
            Column{ .name = "currency_type" },
            Column{ .name = "payment_method", .from_gtfs = from_gtfs.int },
            Column{ .name = "transfers", .from_gtfs = from_gtfs.optionalInt },
            Column{ .name = "agency_id", .from_gtfs = from_gtfs.agencyId },
            Column{ .name = "transfer_duration", .from_gtfs = from_gtfs.optionalInt },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY fare_id",
    },
    Table{
        .gtfs_name = "fare_rules.txt",
        .sql_name = "fare_rules",
        .columns = &[_]Column{
            Column{ .name = "fare_id" },
            Column{ .name = "route_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "origin_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "destination_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "contains_id", .from_gtfs = from_gtfs.optional },
        },
        .order_clause = " ORDER BY fare_id",
    },
    Table{
        .gtfs_name = "shapes.txt",
        .sql_name = "shape_points",
        .columns = &[_]Column{
            Column{ .name = "shape_id" },
            Column{
                .name = "sequence",
                .gtfs_name = "shape_pt_sequence",
                .from_gtfs = from_gtfs.int,
            },
            Column{ .name = "lat", .gtfs_name = "shape_pt_lat", .from_gtfs = from_gtfs.float },
            Column{ .name = "lon", .gtfs_name = "shape_pt_lon", .from_gtfs = from_gtfs.float },
            Column{ .name = "shape_dist_traveled", .from_gtfs = from_gtfs.optionalFloat },
        },
        .parent_implication = ParentImplication{
            .sql_table = "shapes",
            .sql_key = "shape_id",
            .gtfs_key = "shape_id",
        },
        .order_clause = " ORDER BY shape_id, sequence",
    },
    Table{
        .gtfs_name = "trips.txt",
        .sql_name = "trips",
        .required = true,
        .columns = &[_]Column{
            Column{ .name = "trip_id" },
            Column{ .name = "route_id" },
            Column{ .name = "calendar_id", .gtfs_name = "service_id" },
            Column{ .name = "headsign", .gtfs_name = "trip_headsign" },
            Column{ .name = "short_name", .gtfs_name = "trip_short_name" },
            Column{ .name = "direction", .gtfs_name = "direction_id", .from_gtfs = from_gtfs.optionalInt },
            Column{ .name = "block_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "shape_id", .from_gtfs = from_gtfs.optional },
            Column{
                .name = "wheelchair_accessible",
                .from_gtfs = from_gtfs.maybeWithZeroUnknown,
                .to_gtfs = to_gtfs.maybeWithZeroUnknown,
            },
            Column{
                .name = "bikes_allowed",
                .from_gtfs = from_gtfs.maybeWithZeroUnknown,
                .to_gtfs = to_gtfs.maybeWithZeroUnknown,
            },
            Column{
                .name = "exceptional",
                .from_gtfs = from_gtfs.optionalInt,
            },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY trip_id",
    },
    Table{
        .gtfs_name = "stop_times.txt",
        .sql_name = "stop_times",
        .required = true,
        .columns = &[_]Column{
            Column{ .name = "trip_id" },
            Column{ .name = "stop_id" },
            Column{ .name = "stop_sequence", .from_gtfs = from_gtfs.int },
            Column{ .name = "arrival_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "departure_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "pickup_type", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "drop_off_type", .from_gtfs = from_gtfs.intFallbackZero },
            Column{ .name = "stop_headsign" },
            Column{ .name = "shape_dist_traveled", .from_gtfs = from_gtfs.optionalFloat },
            Column{ .name = "platform" },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY trip_id, stop_sequence",
    },
    Table{
        .gtfs_name = "frequencies.txt",
        .sql_name = "frequencies",
        .columns = &[_]Column{
            Column{ .name = "trip_id", .gtfs_name = "trip_id" },
            Column{ .name = "start_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "end_time", .from_gtfs = from_gtfs.time, .to_gtfs = to_gtfs.time },
            Column{ .name = "headway", .gtfs_name = "headway_secs", .from_gtfs = from_gtfs.int },
            Column{ .name = "exact_times", .from_gtfs = from_gtfs.intFallbackZero },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY trip_id, start_time",
    },
    Table{
        .gtfs_name = "transfers.txt",
        .sql_name = "transfers",
        .columns = &[_]Column{
            Column{ .name = "from_stop_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "to_stop_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "from_route_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "to_route_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "from_trip_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "to_trip_id", .from_gtfs = from_gtfs.optional },
            Column{ .name = "transfer_type", .from_gtfs = from_gtfs.int },
            Column{ .name = "min_transfer_time", .from_gtfs = from_gtfs.optionalInt },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY from_stop_id, to_stop_id",
    },
    Table{
        .gtfs_name = "translations.txt",
        .sql_name = "translations",
        .columns = &[_]Column{
            Column{ .name = "table_name" },
            Column{ .name = "field_name" },
            Column{ .name = "language" },
            Column{ .name = "translation" },
            Column{ .name = "record_id" },
            Column{ .name = "record_sub_id" },
            Column{ .name = "field_value" },
        },
        .has_extra_fields_json = true,
        .order_clause = " ORDER BY table_name, record_id, record_sub_id, field_value, field_name, language",
    },
};

/// tablesByGtfsName creates a StaticStringMap mapping GTFS table names
/// (with .txt extensions) to the actual `Table` data.
pub fn tablesByGtfsName() StaticStringMap(*const Table) {
    comptime var kvs_list: [tables.len]struct { []const u8, *const Table } = undefined;
    for (&tables, 0..) |*table, i| {
        kvs_list[i] = .{ table.gtfs_name, table };
    }
    return StaticStringMap(*const Table).initComptime(kvs_list);
}

/// tableByGtfsName finds a `Table` by a given GTFS name (including .txt extension).
pub fn tableByGtfsName(name: []const u8) ?*const Table {
    const tables_by_gtfs_name = comptime tablesByGtfsName();
    return tables_by_gtfs_name.get(name);
}

test "gtfs.table.Table.columnNames" {
    try std.testing.expectEqualStrings(
        "agency_id, name, url, timezone, lang, phone, fare_url",
        comptime tables[0].columnNames(),
    );
}

test "gtfs.table.Table.placeholders" {
    try std.testing.expectEqualStrings(
        "?, ?, ?, ?, ?, ?, ?",
        comptime tables[0].placeholders(),
    );
}

test "gtfs.table.Column.gtfsName" {
    try std.testing.expectEqualStrings(
        "agency_id",
        comptime tables[0].columns[0].gtfsName(),
    );
    try std.testing.expectEqualStrings(
        "agency_name",
        comptime tables[0].columns[1].gtfsName(),
    );
}

test "gtfs.table.Table.gtfsColumnNamesToIndices" {
    const column_by_gtfs_name = comptime tables[0].gtfsColumnNamesToIndices();

    try std.testing.expectEqual(@as(?usize, 0), column_by_gtfs_name.get("agency_id"));
    try std.testing.expectEqual(@as(?usize, 1), column_by_gtfs_name.get("agency_name"));
    try std.testing.expectEqual(@as(?usize, 2), column_by_gtfs_name.get("agency_url"));
    try std.testing.expectEqual(@as(?usize, 3), column_by_gtfs_name.get("agency_timezone"));
    try std.testing.expectEqual(@as(?usize, 4), column_by_gtfs_name.get("agency_lang"));
    try std.testing.expectEqual(@as(?usize, 5), column_by_gtfs_name.get("agency_phone"));
    try std.testing.expectEqual(@as(?usize, 6), column_by_gtfs_name.get("agency_fare_url"));
    try std.testing.expectEqual(@as(?usize, null), column_by_gtfs_name.get("foo"));
    try std.testing.expectEqual(@as(?usize, null), column_by_gtfs_name.get(""));
}

test "gtfs.table.Table.gtfsNameWithoutExtension" {
    try std.testing.expectEqualStrings("agency", tables[0].gtfsNameWithoutExtension());
}
