// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use std::iter::empty;
use std::path::Path;

use rusqlite::types::ValueRef;

use crate::db::open_for_save;
use crate::error::{Result, ResultLocation};
use crate::gtfs::schema::TABLES;
use crate::gtfs::table::{Column, Table};

/// Customizations for the saving process
#[derive(Debug, Default, Clone, Copy)]
pub struct SaveOptions {
    /// Ensure consistent ordering in the output tables?
    /// Whether the [Table::order_clause] should be used when saving data.
    pub ensure_order: bool,

    /// Emit empty/useless rows as well, by skipping the [Table::filter_clause].
    pub emit_empty_rows: bool,
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
    fn select(&self, header: &[&str], options: SaveOptions) -> String {
        // SELECT columns
        let mut stmt = String::from("SELECT ");
        for (i, &gtfs_column_name) in header.iter().enumerate() {
            if i != 0 {
                stmt.push(',');
            }

            if let Some(column) = self.column_by_gtfs_name(gtfs_column_name) {
                stmt.push_str(column.to_gtfs);
            } else if let Some(json_fields_column) = self.json_fields_column() {
                stmt.push_str("json_extract(");
                stmt.push_str(json_fields_column);
                stmt.push_str(",'$.");
                push_json_key_object_path(&mut stmt, gtfs_column_name);
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

        // WHERE
        if let Self::Standard(t) = self
            && !options.emit_empty_rows
        {
            stmt.push_str(t.filter_clause);
        }

        // ORDER BY
        match self {
            Self::Standard(t) => {
                if options.ensure_order {
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
pub fn save<'a>(
    db_path: impl AsRef<Path> + Sync + Send,
    gtfs_path: impl AsRef<Path> + Sync + Send,
    tables: impl IntoIterator<Item = (&'a str, &'a [&'a str])>,
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
                    .with_file(file_name)
                    .inspect_err(|e| log::error!("{}", e))
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
pub fn save_table(
    db_path: impl AsRef<Path>,
    gtfs_path: impl AsRef<Path>,
    table: ResolvedTable,
    header: &[&str],
    options: SaveOptions,
) -> Result<()> {
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_path(gtfs_path.as_ref().join(table.file_name()))?;

    let db: rusqlite::Connection = open_for_save(db_path)?;

    save_table_to_writer(&db, &mut writer, table, header, options)
}

/// Saves a GTFS `table` from a `db` to a `writer`
pub fn save_table_to_writer(
    db: &rusqlite::Connection,
    writer: &mut csv::Writer<impl std::io::Write>,
    table: ResolvedTable,
    header: &[&str],
    options: SaveOptions,
) -> Result<()> {
    // Prepare the SELECT statement and row iterator
    let mut select = db.prepare(&table.select(header, options))?;
    let mut rows = table.query(&mut select)?;

    // Dump header
    writer.write_record(header)?;

    // Dump rows
    while let Some(row) = rows.next()? {
        // Dump each field
        for i in 0..header.len() {
            match row.get_ref_unwrap(i) {
                ValueRef::Null => writer.write_field(b"")?,
                ValueRef::Integer(i) => {
                    let mut b = itoa::Buffer::new();
                    writer.write_field(b.format(i))?;
                }
                ValueRef::Real(f) => {
                    let mut b = zmij::Buffer::new();
                    writer.write_field(b.format(f))?;
                }
                ValueRef::Text(s) => writer.write_field(s)?,
                ValueRef::Blob(b) => writer.write_field(b)?,
            }
        }

        // Mark the end of the row
        writer.write_record(empty::<&[u8]>())?;
    }

    writer.flush()?;
    Ok(())
}

/// Escapes an arbitrary JSON object key into a SQLite's JSON objectlabel, which can be embedded
/// in an SQL string literal.
///
/// This means that most strings are pushed as-is, except for empty strings, or strings
/// containing one of `.["'`. To escape weird keys, the `key` is wrapped in double quotes,
/// double quotes are escaped JSON-style (by preceding with a backslash) and single quotes
/// are escaped SQL-style (by doubling them).
pub fn push_json_key_object_path(dst: &mut String, key: &str) {
    if key.is_empty() {
        dst.push_str("\"\"");
    } else if key.contains(&['"', '.', '[', '\'', '\0']) {
        dst.reserve(key.len() + 2);
        dst.push('"');
        for c in key.chars() {
            match c {
                '\0' => dst.push_str("\\u0000"),
                '"' => dst.push_str("\\\""),
                '\'' => dst.push_str("''"),
                _ => dst.push(c),
            }
        }
        dst.push('"');
    } else {
        dst.push_str(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple() -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();

        const TABLE: ResolvedTable = ResolvedTable::Standard(&TABLES[5]);
        const HEADER: &[&str] = &[
            "agency_id",
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
        ];
        {
            let db = rusqlite::Connection::open_in_memory()?;
            db.execute_batch(concat!(
                "CREATE TABLE agencies (agency_id TEXT PRIMARY KEY) STRICT;",
                "CREATE TABLE routes (",
                "  route_id TEXT PRIMARY KEY,",
                "  agency_id TEXT NOT NULL REFERENCES agencies(agency_id),",
                "  short_name TEXT NOT NULL,",
                "  long_name TEXT NOT NULL,",
                "  type INTEGER NOT NULL,",
                "  color TEXT NOT NULL DEFAULT '',",
                "  text_color TEXT NOT NULL DEFAULT '',",
                "  sort_order INTEGER,",
                "  extra_fields_json TEXT",
                ") STRICT;",
                "INSERT INTO agencies VALUES ('0');",
                "INSERT INTO routes VALUES ('A1', '0', 'A1', 'Warszawa Śródmieście WKD - Grodzisk Mazowiecki Radońska',",
                "  2, '', '', NULL, NULL);",
                "INSERT INTO routes VALUES ('ZA1', '0', 'ZA1', 'Podkowa Leśna Główna - Grodzisk Mazowiecki Radońska (ZKA)',",
                "  3, '', '', NULL, NULL);",
                "INSERT INTO routes VALUES ('ZA12', '0', 'ZA12', 'Podkowa Leśna Główna - Milanówek Grudów (ZKA)',",
                "  3, '', '', NULL, NULL);",
            ))?;

            let mut writer = csv::WriterBuilder::new()
                .terminator(csv::Terminator::CRLF)
                .from_writer(&mut buf);

            save_table_to_writer(&db, &mut writer, TABLE, HEADER, SaveOptions::default())?;
        }

        assert_eq!(
            str::from_utf8(&buf).expect("save must write valid UTF-8"),
            concat!(
                "agency_id,route_id,route_short_name,route_long_name,route_type\r\n",
                "0,A1,A1,Warszawa Śródmieście WKD - Grodzisk Mazowiecki Radońska,2\r\n",
                "0,ZA1,ZA1,Podkowa Leśna Główna - Grodzisk Mazowiecki Radońska (ZKA),3\r\n",
                "0,ZA12,ZA12,Podkowa Leśna Główna - Milanówek Grudów (ZKA),3\r\n",
            )
        );

        Ok(())
    }

    #[test]
    fn test_extra_fields() -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();

        const TABLE: ResolvedTable = ResolvedTable::Standard(&TABLES[0]);
        const HEADER: &[&str] = &[
            "agency_id",
            "agency_name",
            "agency_url",
            "agency_timezone",
            "agency_lang",
            "agency_email",
        ];
        {
            let db = rusqlite::Connection::open_in_memory()?;
            db.execute_batch(concat!(
                "CREATE TABLE agencies (",
                "  agency_id TEXT PRIMARY KEY,",
                "  name TEXT NOT NULL,",
                "  url TEXT NOT NULL,",
                "  timezone TEXT NOT NULL,",
                "  lang TEXT NOT NULL DEFAULT '',",
                "  phone TEXT NOT NULL DEFAULT '',",
                "  fare_url TEXT NOT NULL DEFAULT '',",
                "  extra_fields_json TEXT",
                ") STRICT;",
                "INSERT INTO agencies VALUES ('0', 'Foo', 'https://example.com', 'UTC',",
                r#"  'en', '', '', '{"agency_email":"foo@example.com"}');"#,
                "INSERT INTO agencies VALUES ('1', 'Bar', 'https://example.com', 'UTC',",
                r#"  'en', '', '', NULL);"#,
            ))?;

            let mut writer = csv::WriterBuilder::new()
                .terminator(csv::Terminator::CRLF)
                .from_writer(&mut buf);

            save_table_to_writer(&db, &mut writer, TABLE, HEADER, SaveOptions::default())?;
        }

        assert_eq!(
            str::from_utf8(&buf).expect("save must write valid UTF-8"),
            concat!(
                "agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_email\r\n",
                "0,Foo,https://example.com,UTC,en,foo@example.com\r\n",
                "1,Bar,https://example.com,UTC,en,\r\n",
            )
        );

        Ok(())
    }

    #[test]
    fn test_extra_files() -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();

        const TABLE: ResolvedTable = ResolvedTable::Extra("foo.txt");
        const HEADER: &[&str] = &["foo", "bar", "spam"];
        {
            let db = rusqlite::Connection::open_in_memory()?;
            db.execute_batch(concat!(
                "CREATE TABLE extra_table_rows (",
                "  extra_table_row_id INTEGER PRIMARY KEY,",
                "  table_name TEXT NOT NULL,",
                "  fields_json TEXT NOT NULL DEFAULT '{}',",
                "  row_sort_order INTEGER",
                ") STRICT;",
                "INSERT INTO extra_table_rows (table_name, fields_json, row_sort_order) VALUES ",
                r#"  ('foo.txt', '{"foo":"1","bar":"Hello","baz":"42"}', 1);"#,
                "INSERT INTO extra_table_rows (table_name, fields_json, row_sort_order) VALUES ",
                r#"  ('foo.txt', '{"foo":"2","bar":"World","baz":""}', 2);"#,
                "INSERT INTO extra_table_rows (table_name, fields_json, row_sort_order) VALUES ",
                r#"  ('bar.txt', '{"spam":"eggs"}', 1);"#,
            ))?;

            let mut writer = csv::WriterBuilder::new()
                .terminator(csv::Terminator::CRLF)
                .from_writer(&mut buf);

            save_table_to_writer(&db, &mut writer, TABLE, HEADER, SaveOptions::default())?;
        }

        assert_eq!(
            str::from_utf8(&buf).expect("save must write valid UTF-8"),
            "foo,bar,spam\r\n1,Hello,\r\n2,World,\r\n",
        );

        Ok(())
    }

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
                SaveOptions {
                    ensure_order: true,
                    emit_empty_rows: false
                },
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
            t.select(
                &["variant_id", "route_id", "variant_code"],
                SaveOptions {
                    ensure_order: true,
                    emit_empty_rows: false
                },
            ),
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

    fn json_key_to_object_path(key: &str) -> String {
        let mut s = String::new();
        push_json_key_object_path(&mut s, key);
        s
    }

    #[test]
    fn test_json_key_to_object_path() {
        assert_eq!(json_key_to_object_path("foo"), "foo");
        assert_eq!(json_key_to_object_path("bar123_baz"), "bar123_baz");
        assert_eq!(json_key_to_object_path(""), "\"\"");
        assert_eq!(json_key_to_object_path("foo.bar"), "\"foo.bar\"");
        assert_eq!(
            json_key_to_object_path("why[are]there[brackets]"),
            "\"why[are]there[brackets]\""
        );
        assert_eq!(
            json_key_to_object_path("super.\"wonky\".'string'!!!"),
            "\"super.\\\"wonky\\\".''string''!!!\""
        );
    }
}
