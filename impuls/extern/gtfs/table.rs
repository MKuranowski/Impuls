// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use rusqlite::types::ValueRef;

/// Definition of a well-known GTFS/Impuls (SQL) table
pub struct Table<'a> {
    /// Name of the Impuls (SQL) table name
    pub sql_name: &'a str,

    /// Name of the file in the GTFS dataset, including the .txt extension
    pub gtfs_name: &'a str,

    /// Mapping details between SQL and GTFS schemas
    pub columns: &'a [Column<'a>],

    /// Does this table need to be present in order to consider a GTFS dataset valid?
    pub required: bool,

    /// Whether rows of this GTFS table imply a "virtual" object, which must be inserted
    /// before the actual row.
    ///
    /// For example, entries in shapes.txt imply the existence of a "shape".
    /// Before inserting into shape_points in SQL, an extra INSERT INTO shapes
    /// needs to occur.
    pub parent_implication: Option<ParentImplication<'a>>,

    /// Does this table have an "extra_fields_json" column?
    ///
    /// Note that this column is never present in [Table::columns].
    pub has_extra_fields_json: bool,

    /// Optional " ORDER BY ..." clause to enforce a specific order of objects in
    /// this table. Note that the leading space is required.
    pub order_clause: &'a str,
}

impl<'a> Table<'a> {
    /// Finds a column with the provided [gtfs_name](Table::gtfs_name), by linearly scanning
    /// [columns](Table::columns).
    pub fn column_by_gtfs_name(&self, gtfs_name: &str) -> Option<&'a Column<'a>> {
        self.columns.iter().find(|&c| c.gtfs_name == gtfs_name)
    }
}

/// Definition of a well-known GTFS/Impuls (SQL) column of a [Table]
pub struct Column<'a> {
    /// Name of the column in the Impuls (SQL) schema
    pub _sql_name: &'a str,

    /// Name of the column in GTFS
    pub gtfs_name: &'a str,

    /// SQL Expression converting an SQL value into a GTFS compliant value
    pub to_gtfs: &'a str,

    /// SQL Expression converting a GTFS string parameter ("?") into a valid
    /// value which can be stored in the database. "?" must appear exactly once.
    pub from_gtfs: &'a str,

    /// Fallback value to bind when this column is not present in the GTFS or is empty.
    pub from_fallback: FallbackValue<'a>,
}

impl<'a> Column<'a> {
    /// Creates a new Column with the same GTFS and Impuls (SQL) names, no extra conversion
    /// (to_gtfs=`$name`, from_gtfs=`?`), and [no fallback value](FallbackValue::AsIs).
    pub const fn new(name: &'a str) -> Self {
        Self {
            _sql_name: name,
            gtfs_name: name,
            to_gtfs: name,
            from_gtfs: "?",
            from_fallback: FallbackValue::AsIs,
        }
    }

    /// Creates a new Column with differing GTFS and Impuls (SQL) names, but no extra conversion
    /// (to_gtfs=`$sql_name`, from_gtfs=`?`), and [no fallback value](FallbackValue::AsIs).
    pub const fn with_names(sql_name: &'a str, gtfs_name: &'a str) -> Self {
        Self {
            _sql_name: sql_name,
            gtfs_name,
            to_gtfs: sql_name,
            from_gtfs: "?",
            from_fallback: FallbackValue::AsIs,
        }
    }

    /// Creates a new Column with the same GTFS and Impuls (SQL) names and the provided
    /// fallback value, but no extra conversion (to_gtfs=`$name`, from_gtfs=`?`).
    pub const fn with_fallback(name: &'a str, from_fallback: FallbackValue<'a>) -> Self {
        Self {
            _sql_name: name,
            gtfs_name: name,
            to_gtfs: name,
            from_gtfs: "?",
            from_fallback,
        }
    }

    /// Creates a new Column with differing GTFS and Impuls (SQL) names and the provided
    /// fallback value, but no extra conversion (to_gtfs=`$sql_name`, from_gtfs=`?`).
    pub const fn with_names_and_fallback(
        sql_name: &'a str,
        gtfs_name: &'a str,
        from_fallback: FallbackValue<'a>,
    ) -> Self {
        Self {
            _sql_name: sql_name,
            gtfs_name,
            to_gtfs: sql_name,
            from_gtfs: "?",
            from_fallback,
        }
    }
}

/// Value to use in place of empty/missing GTFS cells.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FallbackValue<'a> {
    /// Leave the empty/missing value as is. Default option, equivalent to `Str("")`.
    AsIs,

    /// Use this specific string in place of empty/missing values.
    Str(&'a [u8]),

    /// Use this specific integer in place of empty/missing values.
    Int(i64),

    /// Use "NULL" in place of empty/missing values. Particularly useful for optional keys,
    /// where empty strings would be interpreted literally as a key and cause a foreign key
    /// violation.
    Null,

    /// Use the CSV line number in place of empty/missing values.
    LineNum,
}

impl<'a> Default for FallbackValue<'a> {
    fn default() -> Self {
        Self::AsIs
    }
}

impl FallbackValue<'_> {
    /// Resolve the value to insert, from the provided cell contents and CSV line number.
    ///
    /// Returns `ValueRef::Text(cell)` when cell is non-empty, and an appropriate fallback
    /// value otherwise.
    pub fn fill<'cell>(&'cell self, cell: &'cell [u8], line_num: u64) -> ValueRef<'cell> {
        if !cell.is_empty() {
            ValueRef::Text(cell)
        } else {
            match self {
                Self::AsIs => ValueRef::Text(cell),
                Self::Str(fallback) => ValueRef::Text(fallback),
                Self::Int(fallback) => ValueRef::Integer(*fallback),
                Self::Null => ValueRef::Null,
                Self::LineNum => ValueRef::Integer(line_num as i64),
            }
        }
    }
}

/// Implication of a parent row; created by executing
/// `INSERT INTO $sql_table ($sql_column) VALUES ${gtfs_row[gtfs_column]}`
#[derive(Debug)]
pub struct ParentImplication<'a> {
    /// Name of the SQL table in which the implied parent lives
    pub sql_table: &'a str,

    /// SQL column containing the primary key of the parent sql_table.
    pub sql_column: &'a str,

    /// GTFS column containing the foreign key in the child table.
    /// Values of that column will be used to insert rows into the parent table.
    pub gtfs_column: &'a str,
}

#[cfg(test)]
mod tests {
    use rusqlite::types::ValueRef;

    use super::FallbackValue;

    #[test]
    fn test_fallback_value_fill() {
        assert_eq!(FallbackValue::AsIs.fill(b"foo", 42), ValueRef::Text(b"foo"));
        assert_eq!(FallbackValue::AsIs.fill(b"", 42), ValueRef::Text(b""));

        assert_eq!(
            FallbackValue::Str(b"filler").fill(b"foo", 42),
            ValueRef::Text(b"foo"),
        );
        assert_eq!(
            FallbackValue::Str(b"filler").fill(b"", 42),
            ValueRef::Text(b"filler"),
        );

        assert_eq!(FallbackValue::Int(0).fill(b"2", 42), ValueRef::Text(b"2"));
        assert_eq!(FallbackValue::Int(0).fill(b"", 42), ValueRef::Integer(0));

        assert_eq!(FallbackValue::Null.fill(b"foo", 42), ValueRef::Text(b"foo"));
        assert_eq!(FallbackValue::Null.fill(b"", 42), ValueRef::Null);

        assert_eq!(
            FallbackValue::LineNum.fill(b"foo", 42),
            ValueRef::Text(b"foo"),
        );
        assert_eq!(FallbackValue::LineNum.fill(b"", 42), ValueRef::Integer(42));
    }
}
