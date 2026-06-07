// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use std::iter::empty;
use std::path::Path;

use rusqlite::types::ValueRef;

use crate::db::open_for_save;
use crate::error::Result;
use crate::gtfs::schema::TABLES;
use crate::gtfs::table::{Column, Table};

/// Customizations for the saving process
#[derive(Debug, Default, Clone, Copy)]
pub struct SaveOptions {
    /// Ensure consistent ordering in the output tables?
    /// Whether the [Table::order_clause] should be used when saving data.
    pub ensure_order: bool,
}

/// How to access data for a requested output table?
pub enum ResolvedTable<'a> {
    /// The requested table is well-known - use [Table] info.
    Standard(&'a Table<'a>),

    /// The requested table is unspecified - use `extra_table_rows` with `table_name` equal to the
    /// provided string.
    Extra(&'a str),
}

impl<'a> ResolvedTable<'a> {
    /// Resolves a table. If the file_name is present in the global [TABLES] directory
    /// (using [Table::gtfs_name]), returns [ResolvedTable::Standard], and [ResolvedTable::Extra]
    /// otherwise.
    pub fn new(file_name: &'a str) -> Self {
        TABLES
            .iter()
            .find(|t| t.gtfs_name == file_name)
            .map(|t| Self::Standard(t))
            .unwrap_or(Self::Extra(file_name))
    }

    /// Returns the GTFS file name of the table.
    fn file_name(&self) -> &str {
        match self {
            Self::Standard(t) => t.gtfs_name,
            Self::Extra(name) => name,
        }
    }

    /// Attempts to find the definition of a [Column] by its [gtfs_name](Column::gtfs_name).
    ///
    /// Return `None` for extra tables and extra columns in standard tables.
    fn column_by_gtfs_name(&self, gtfs_name: &str) -> Option<&'a Column<'a>> {
        match self {
            Self::Standard(t) => t.column_by_gtfs_name(gtfs_name),
            Self::Extra(_) => None,
        }
    }

    /// Returns the name of the column containing a JSON (`{"name":"value"}`) lookup for
    /// generic column storage.
    ///
    /// That is - `fields_json` for extra tables, `extra_fields_json` for standard tables
    /// with [Table::has_extra_fields_json] and None otherwise.
    fn json_fields_column(&self) -> Option<&'static str> {
        match self {
            Self::Standard(t) => {
                if t.has_extra_fields_json {
                    Some("extra_fields_json")
                } else {
                    None
                }
            }
            Self::Extra(_) => Some("fields_json"),
        }
    }

    /// Generates a "SELECT ... FROM ... " SQL statement for dumping data for this table,
    /// matching with the provided GTFS header. Values returned by this statement will be
    /// ready to be dumped to GTFS (as-in - they are converted with [Column::to_gtfs]).
    ///
    /// The generated statement may have placeholders. Use [ResolvedTable::query] to actually
    /// execute this statement.
    ///
    /// Extra columns (applied to all columns of extra tables) must match `[A-Za-z0-9_-]+`,
    /// otherwise they are replaced by NULL. NULLs are also used for tables without
    /// generic extra columns (see [Table::has_extra_fields_json]).
    fn select(&self, header: &[&str], ensure_order: bool) -> String {
        // SELECT columns
        let mut stmt = String::from("SELECT ");
        for (i, &gtfs_column_name) in header.iter().enumerate() {
            if i != 0 {
                stmt.push(',');
            }

            if let Some(column) = self.column_by_gtfs_name(gtfs_column_name) {
                stmt.push_str(column.to_gtfs);
            } else if let Some(json_fields_column) = self.json_fields_column()
                && !gtfs_column_name.is_empty()
                && gtfs_column_name.chars().all(is_safe_object_path)
            {
                stmt.push_str("json_extract(");
                stmt.push_str(json_fields_column);
                stmt.push_str(",'$.");
                stmt.push_str(gtfs_column_name);
                stmt.push_str("')");
            } else {
                stmt.push_str("NULL");
            }
        }

        // FROM
        stmt.push_str(" FROM ");
        match self {
            Self::Standard(t) => stmt.push_str(t.sql_name),
            Self::Extra(_) => stmt.push_str("extra_table_rows WHERE table_name = ?"),
        }

        // ORDER BY
        match self {
            Self::Standard(t) => {
                if ensure_order {
                    stmt.push_str(t.order_clause)
                }
            }
            Self::Extra(_) => stmt.push_str(" ORDER BY row_sort_order"),
        }

        // All done :^)
        stmt
    }

    /// Executes a compiled [ResolvedTable::select] statement, by correctly binding
    /// parameters. The statement must not outlive this [ResolvedTable].
    fn query<'b: 'a>(
        &self,
        stmt: &'b mut rusqlite::Statement,
    ) -> rusqlite::Result<rusqlite::Rows<'b>> {
        match self {
            Self::Standard(_) => stmt.query(()),
            Self::Extra(table_name) => stmt.query((table_name,)),
        }
    }
}

/// Saves a GTFS dataset from an Impuls SQLite database at `db_path` to a directory
/// at `gtfs_path` according to the provided `options`.
///
/// `tables` must be an iterator of (file_name, gtfs_header). It's the callers responsibility
/// to ensure file_names are considered valid (for example, barring paths reaching out with `../`).
/// If the file name points to a subdirectory, the caller must create that subdirectory beforehand.
///
/// In general however, it's recommended to only allow flat filenames (`[A-Za-z0-9_.-]+`) and
/// ensure `gtfs_path` is an empty directory.
///
/// Each table is saved by calling [save_table] in its own thread. If multiple calls fail,
/// all errors are [logged](log::error!), but only the first error is returned (in the order of
/// `tables`).
pub fn save<
    'a,
    P1: AsRef<Path> + Sync + Send,
    P2: AsRef<Path> + Sync + Send,
    I: IntoIterator<Item = (&'a str, &'a [&'a str])>,
>(
    db_path: P1,
    gtfs_path: P2,
    tables: I,
    options: SaveOptions,
) -> Result<()> {
    std::thread::scope(|s| {
        // Ensure borrows of paths are "moved" into the thread closure, not the original values
        let db_path = &db_path;
        let gtfs_path = &gtfs_path;

        // Spawn save_table threads
        let handles: Vec<_> = tables
            .into_iter()
            .map(|(file_name, header)| {
                s.spawn(move || {
                    save_table(
                        db_path,
                        gtfs_path,
                        ResolvedTable::new(file_name),
                        header,
                        options,
                    )
                    .inspect_err(|e| log::error!("{}: {}", file_name, e))
                })
            })
            .collect();

        // Collect thread results, and preserve the first error if any
        handles.into_iter().fold(Ok(()), |result, handle| {
            let thread_result = handle.join().expect("save_table should not panic");

            // Keep the first encountered error
            if result.is_err() {
                result
            } else {
                thread_result
            }
        })
    })
}

/// Saves a GTFS `table` from an Impuls SQLite database at `db_path` to a file at
/// `gtfs_path.join(table.file_name())`.
pub fn save_table<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    gtfs_path: P2,
    table: ResolvedTable,
    header: &[&str],
    options: SaveOptions,
) -> Result<()> {
    // Open the CSV file and write its header
    let mut w = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_path(gtfs_path.as_ref().join(table.file_name()))?;
    w.write_record(header)?;

    // Open the DB, and compile and execute the SELECT query
    let db: rusqlite::Connection = open_for_save(db_path)?;
    let mut select = db.prepare(&table.select(header, options.ensure_order))?;
    let mut rows = table.query(&mut select)?;

    // Dump rows
    while let Some(row) = rows.next()? {
        // Dump each field
        for i in 0..header.len() {
            match row.get_ref_unwrap(i) {
                ValueRef::Null => w.write_field(b"")?,
                ValueRef::Integer(i) => {
                    let mut b = itoa::Buffer::new();
                    w.write_field(b.format(i))?;
                }
                ValueRef::Real(f) => {
                    let mut b = zmij::Buffer::new();
                    w.write_field(b.format(f))?;
                }
                ValueRef::Text(s) => w.write_field(s)?,
                ValueRef::Blob(b) => w.write_field(b)?,
            }
        }

        // Mark the end of the row
        w.write_record(empty::<&[u8]>())?;
    }

    w.flush()?;
    Ok(())
}

fn is_safe_object_path(c: char) -> bool {
    match c {
        'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_table_standard() {
        let t = ResolvedTable::new("agency.txt");
        assert!(matches!(t, ResolvedTable::Standard(..)));
        assert_eq!(t.file_name(), "agency.txt");
        assert!(t.column_by_gtfs_name("agency_name").is_some());
        assert!(t.column_by_gtfs_name("route_type").is_none());
        assert_eq!(t.json_fields_column(), Some("extra_fields_json"));
        assert_eq!(
            t.select(
                &[
                    "agency_id",
                    "agency_name",
                    "agency_url",
                    "agency_timezone",
                    "agency_address"
                ],
                true,
            ),
            concat!(
                "SELECT agency_id,name,url,timezone,",
                "json_extract(extra_fields_json,'$.agency_address') ",
                "FROM agencies ",
                "ORDER BY agency_id",
            ),
        );
    }

    #[test]
    fn test_resolved_table_extra() {
        let t = ResolvedTable::new("variants.txt");
        assert!(matches!(t, ResolvedTable::Extra(..)));
        assert_eq!(t.file_name(), "variants.txt");
        assert!(t.column_by_gtfs_name("route_id").is_none());
        assert_eq!(t.json_fields_column(), Some("fields_json"));
        assert_eq!(
            t.select(&["variant_id", "route_id", "variant_code"], false),
            concat!(
                "SELECT ",
                "json_extract(fields_json,'$.variant_id'),",
                "json_extract(fields_json,'$.route_id'),",
                "json_extract(fields_json,'$.variant_code') ",
                "FROM extra_table_rows ",
                "WHERE table_name = ? ",
                "ORDER BY row_sort_order",
            ),
        );
    }
}
