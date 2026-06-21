// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use std::fs::File;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::Path;

use rusqlite::params_from_iter;
use rusqlite::types::{ToSqlOutput, ValueRef};

use crate::db;
use crate::error::{Result, ResultLocation};
use crate::gtfs::schema::TABLES;
use crate::gtfs::table::{Column, Table};

/// Customizations for the loading process
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LoadOptions {
    /// Load the [extra_fields_json](Table::has_extra_fields_json) column where available with
    /// extra columns, not present in [Table::columns].
    pub extra_fields: bool,
}

/// Loads a GTFS dataset from an unpacked directory at `gtfs_path` into an
/// Impuls SQLite database at `db_path`.
///
/// By default, only well-known columns of well-known files (from [TABLES]) are loaded.
///
/// Any extra files are ignored, unless provided in the `extra_files` argument. It's the caller's
/// responsibility to ensure those file names are considered valid (for example, barring paths
/// reaching out with `../`). Only a warning will be printed if those files are missing.
pub fn load<'a>(
    db_path: impl AsRef<Path>,
    gtfs_path: impl AsRef<Path>,
    extra_files: impl IntoIterator<Item = &'a str>,
    options: LoadOptions,
) -> Result<()> {
    let mut db = db::open_for_load(&db_path)?;

    for table in TABLES {
        load_table::<StandardInserter>(&mut db, &gtfs_path, table, options)
            .with_file(table.gtfs_name)?;
    }

    for extra_file_name in extra_files {
        load_table::<ExtraInserter>(&mut db, &gtfs_path, extra_file_name, options)
            .with_file(extra_file_name)?;
    }

    Ok(())
}

/// Loads a single GTFS table (file) using the provided [Inserter], from
/// `gtfs_path.join(I::file_name(&data))`.
pub fn load_table<'a, I: Inserter<'a>>(
    db: &'a rusqlite::Connection,
    gtfs_path: impl AsRef<std::path::Path>,
    data: I::TableData,
    options: LoadOptions,
) -> Result<()> {
    let file_name = I::file_name(&data);
    let required = I::is_required(&data);
    log::debug!("Loading {}", file_name);

    // Open the file
    let reader = match open_reader(gtfs_path.as_ref().join(file_name)) {
        Err(e) if !required && e.has_io_kind(std::io::ErrorKind::NotFound) => return Ok(()),
        Err(e) => return Err(e),
        Ok(r) => r,
    };

    // Initialize the inserter
    let inner = I::new(db, data, options)?;

    // Initialize the loader and run the process
    Loader::new(inner, reader).load_file(db)?;

    Ok(())
}

/// Creates a default [csv::Reader] for a GTFS file at the provided path.
fn open_reader<P: AsRef<Path>>(p: P) -> Result<csv::Reader<File>> {
    let r = csv::ReaderBuilder::new().has_headers(true).from_path(p)?;
    Ok(r)
}

/// Loader manages processing of entire files, passing that data through to an [Inserter],
/// chunking calls in [transactions](rusqlite::Transaction).
pub struct Loader<'a, I: Inserter<'a>, R: std::io::Read> {
    inserter: I,
    reader: csv::Reader<R>,
    lifetime: PhantomData<&'a ()>,
}

impl<'a, I: Inserter<'a>, R: std::io::Read> Loader<'a, I, R> {
    /// Creates a new Loader from the provided [Inserter] and a CSV reader.
    pub fn new(inserter: I, reader: csv::Reader<R>) -> Self {
        Self {
            inserter,
            reader,
            lifetime: PhantomData::default(),
        }
    }

    /// Loads the entire file, creating a separate [transaction](rusqlite::Transaction)
    /// for a sensible chunk of rows from the file, and calling back [Inserter::load_row].
    pub fn load_file(&mut self, db: &'a rusqlite::Connection) -> Result<()> {
        let column_data = self.load_header().with_line(1)?;
        self.load_rows(db, &column_data)
    }

    fn load_header(&mut self) -> Result<Vec<I::ColumnData>> {
        let header = self.reader.headers()?;
        let columns: Vec<_> = header
            .into_iter()
            .map(|column_name| self.inserter.parse_header_cell(column_name))
            .collect();
        Ok(columns)
    }

    fn load_rows(
        &mut self,
        db: &'a rusqlite::Connection,
        column_data: &[I::ColumnData],
    ) -> Result<()> {
        let mut row = csv::ByteRecord::new();
        while self.load_rows_chunk(db, column_data, &mut row)? {}
        Ok(())
    }

    fn load_rows_chunk(
        &mut self,
        db: &'a rusqlite::Connection,
        column_data: &[I::ColumnData],
        row: &mut csv::ByteRecord,
    ) -> Result<bool> {
        const CHUNK_SIZE: usize = 100_000;
        let mut has_rows = true;
        let mut rows = 0_usize;
        let tx = db.unchecked_transaction()?;

        while has_rows && rows < CHUNK_SIZE {
            has_rows = self
                .reader
                .read_byte_record(row)
                .with_line(self.reader.position().line())?;

            if has_rows {
                rows += 1;
                let line = row.position().unwrap_or(self.reader.position()).line();
                self.inserter
                    .insert_row(row.iter(), column_data, line)
                    .with_line(line)?;
            }
        }

        tx.commit()?;
        Ok(has_rows)
    }
}

/// Inserter manages processing of individual rows.
pub trait Inserter<'a>: Sized {
    /// TableData represent some sort of description of the table this inserter is processing.
    /// It outlives the inserter.
    type TableData: 'a;

    /// ColumnData represent some sort of description of a column this inserter is processing.
    type ColumnData: 'a;

    /// Infers the file name in a GTFS directory of the table to be processed from `TableData`.
    fn file_name(data: &Self::TableData) -> &str;

    /// Infers if an error should be raised when this GTFS table is missing.
    fn is_required(data: &Self::TableData) -> bool;

    /// Instantiates a new Inserter, for example by compiling INSERT statements.
    fn new(
        db: &'a rusqlite::Connection,
        data: Self::TableData,
        options: LoadOptions,
    ) -> Result<Self>;

    /// Annotates a column, given its GTFS name.
    fn parse_header_cell(&self, column_name: &str) -> Self::ColumnData;

    /// Insert row processes a row, for example by executing a previously compiled INSERT
    /// statement. `row` and `columns` must have the same lengths.
    fn insert_row<'row>(
        &mut self,
        row: impl IntoIterator<Item = &'row [u8]>,
        columns: &[Self::ColumnData],
        line: u64,
    ) -> Result<()>;
}

/// How to load a GTFS column of a well-known [Table].
///
/// Used as [Inserter::ColumnData] by [StandardInserter].
pub enum ColumnMapping<'a> {
    /// GTFS column maps to the ith entry in [Table::columns] - ith parameter to [TableLoader::insert]
    Standard(u8, &'a Column<'a>),

    /// GTFS column maps to the ith entry in [Table::columns] - ith parameter to [TableLoader::insert],
    /// and the sole parameter to [StandardInserter::parent_insert].
    StandardAndParent(u8, &'a Column<'a>),

    /// GTFS column maps to the generic extra_fields_json/fields_json - last parameter to [TableLoader::insert].
    Extra(String),

    /// GTFS column doesn't map to anything.
    None,
}

/// Inserter of rows into a well-known [Table].
pub struct StandardInserter<'a> {
    /// [Table] being processed.
    table: &'a Table<'a>,

    /// "INSERT INTO [Table::sql_name] ..." column. The first parameters correspond exactly
    /// to [Table::columns].
    insert: rusqlite::Statement<'a>,

    /// Optional "INSERT OR IGNORE INTO [ParentImplication::sql_table]". Has exactly one
    /// parameter, corresponding to [ParentImplication::gtfs_column].
    parent_insert: Option<rusqlite::Statement<'a>>,

    /// When non-empty, [StandardInserter::insert] has one extra trailing parameter,
    /// corresponding to the `extra_fields_json` columns. This buffer can be re-used
    /// by rows to build the JSON field mapping.
    extra_fields_buffer: Option<GenericFieldsBuilder>,

    /// Default parameters for every INSERT. For every row this value is copied,
    /// values present in GTFS are substituted (through [Column::from_fallback])
    /// (so values not present in the GTFS are passed through from this array),
    /// bind to [StandardInserter::insert] and executed.
    ///
    /// 256 elements are chosen deliberately so that the compiler can omit bound checking
    /// when indexed with a u8.
    initial_params: [ValueRef<'a>; 256],

    /// Indices into [StandardInserter::initial_params] which need to be set per-row with the value
    /// of the current line number.
    line_number_params: Vec<u8>,
}

impl<'a> Inserter<'a> for StandardInserter<'a> {
    type TableData = &'a Table<'a>;
    type ColumnData = ColumnMapping<'a>;

    fn file_name(data: &Self::TableData) -> &str {
        data.gtfs_name
    }

    fn is_required(data: &Self::TableData) -> bool {
        data.required
    }

    fn new(
        db: &'a rusqlite::Connection,
        table: Self::TableData,
        options: LoadOptions,
    ) -> Result<Self> {
        let has_extra_fields = table.has_extra_fields_json && options.extra_fields;

        Ok(Self {
            table,
            insert: Self::compile_insert(db, table, has_extra_fields)?,
            parent_insert: Self::compile_parent_insert(db, table)?,
            extra_fields_buffer: if has_extra_fields {
                Some(GenericFieldsBuilder::new())
            } else {
                None
            },
            initial_params: table.all_fallback_values(),
            line_number_params: table.line_num_fallback_values(),
        })
    }

    fn parse_header_cell(&self, column_name: &str) -> Self::ColumnData {
        // Try to find the matching entry in [Table::columns]
        let idx = self
            .table
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.gtfs_name == column_name);

        let is_parent = self
            .table
            .parent_implication
            .as_ref()
            .map_or(false, |pi| pi.gtfs_column == column_name);

        let has_extra = self.extra_fields_buffer.is_some();

        match (idx, is_parent) {
            (Some((idx, col)), true) => {
                let idx: u8 = idx
                    .try_into()
                    .expect("too many columns - expected at most 255");
                ColumnMapping::StandardAndParent(idx, col)
            }

            (Some((idx, col)), _) => {
                let idx: u8 = idx
                    .try_into()
                    .expect("too many columns - expected at most 255");
                ColumnMapping::Standard(idx, col)
            }

            (None, true) => panic!(
                "Table.parent_implication.gtfs_column refers to a column which does not exist in Table.columns"
            ),

            (None, _) if has_extra => ColumnMapping::Extra(column_name.to_string()),
            (None, _) => ColumnMapping::None,
        }
    }

    fn insert_row<'row>(
        &mut self,
        row: impl IntoIterator<Item = &'row [u8]>,
        columns: &[Self::ColumnData],
        line: u64,
    ) -> Result<()> {
        // Clear any bindings
        let mut params = self.initial_params;
        let mut pi_param = ValueRef::Null;
        self.clear_extra_fields();

        // Fill fallback line number values
        for &idx in &self.line_number_params {
            params[idx as usize] = ValueRef::Integer(line as i64);
        }

        // Push bindings
        for (cell, col) in row.into_iter().zip(columns) {
            match col {
                ColumnMapping::Standard(idx, data) => {
                    params[*idx as usize] = data.from_fallback.fill(cell, line);
                }

                ColumnMapping::StandardAndParent(idx, data) => {
                    params[*idx as usize] = data.from_fallback.fill(cell, line);
                    pi_param = ValueRef::Text(cell);
                }

                ColumnMapping::Extra(key) => self.push_extra_field(&key, cell),

                ColumnMapping::None => {}
            }
        }

        // Push the extra fields parameter
        let mut columns = self.table.columns.len();
        self.finish_extra_fields();
        if let Some(extra_fields) = &self.extra_fields_buffer {
            // Leave the bindings as NULL when there are no extra fields
            if !extra_fields.is_empty() {
                params[columns] = ValueRef::Text(extra_fields.as_ref())
            }
            columns += 1;
        }

        // Execute the parent insert
        if let Some(parent_insert) = &mut self.parent_insert {
            parent_insert.execute((ToSqlOutput::Borrowed(pi_param),))?;
        }

        // Execute the actual insert
        self.insert.execute(params_from_iter(
            params[0..columns].iter().map(|&v| ToSqlOutput::Borrowed(v)),
        ))?;

        Ok(())
    }
}

impl<'a> StandardInserter<'a> {
    fn compile_insert(
        db: &'a rusqlite::Connection,
        table: &Table,
        has_extra_fields: bool,
    ) -> Result<rusqlite::Statement<'a>> {
        Ok(db.prepare(&Self::prepare_insert(table, has_extra_fields))?)
    }

    fn prepare_insert(table: &Table, has_extra_fields: bool) -> String {
        let mut insert = String::new();
        insert.push_str("INSERT INTO ");
        insert.push_str(table.sql_name);
        insert.push_str(" (");
        for (i, col) in table.columns.iter().enumerate() {
            if i != 0 {
                insert.push(',');
            }
            insert.push_str(col.sql_name);
        }

        if has_extra_fields {
            insert.push_str(",extra_fields_json");
        }

        insert.push_str(") VALUES (");
        for (i, col) in table.columns.iter().enumerate() {
            if i != 0 {
                insert.push(',');
            }
            insert.push_str(col.from_gtfs);
        }
        if has_extra_fields {
            if table.columns.is_empty() {
                insert.push('?');
            } else {
                insert.push_str(",?");
            }
        }

        insert.push(')');
        insert
    }

    fn compile_parent_insert(
        db: &'a rusqlite::Connection,
        table: &Table,
    ) -> Result<Option<rusqlite::Statement<'a>>> {
        match Self::prepare_parent_insert(table) {
            Some(sql) => Ok(Some(db.prepare(&sql)?)),
            None => Ok(None),
        }
    }

    fn prepare_parent_insert(table: &Table) -> Option<String> {
        table.parent_implication.as_ref().map(|pi| {
            let mut insert = String::new();
            insert.push_str("INSERT OR IGNORE INTO ");
            insert.push_str(pi.sql_table);
            insert.push_str(" (");
            insert.push_str(pi.sql_column);
            insert.push_str(") VALUES (?)");
            insert
        })
    }

    fn clear_extra_fields(&mut self) {
        if let Some(buf) = &mut self.extra_fields_buffer {
            buf.clear();
        }
    }

    fn push_extra_field(&mut self, k: &str, v: &[u8]) {
        if let Some(buf) = &mut self.extra_fields_buffer {
            buf.push_kv(k.as_bytes(), v);
        }
    }

    fn finish_extra_fields(&mut self) {
        if let Some(buf) = &mut self.extra_fields_buffer {
            buf.finalize();
        }
    }
}

/// Inserter of rows into an unknown table - into `extra_table_rows`
pub struct ExtraInserter<'a> {
    /// Name of the extra file, used for the `table_name` column
    file_name: &'a str,

    /// Compiled "INSERT INTO extra_table_rows" statement with 3 parameters:
    /// table_name, fields_json and row_sort_order.
    insert: rusqlite::Statement<'a>,

    /// Buffer for building `fields_json`.
    fields_buffer: GenericFieldsBuilder,
}

impl<'a> Inserter<'a> for ExtraInserter<'a> {
    type TableData = &'a str;
    type ColumnData = String;

    fn file_name(data: &Self::TableData) -> &str {
        data
    }

    fn is_required(_data: &Self::TableData) -> bool {
        false
    }

    fn new(
        db: &'a rusqlite::Connection,
        data: Self::TableData,
        _options: LoadOptions,
    ) -> Result<Self> {
        Ok(Self {
            file_name: data,
            insert: Self::compile_insert(db)?,
            fields_buffer: GenericFieldsBuilder::new(),
        })
    }

    fn parse_header_cell(&self, column_name: &str) -> Self::ColumnData {
        column_name.to_string()
    }

    fn insert_row<'row>(
        &mut self,
        row: impl IntoIterator<Item = &'row [u8]>,
        columns: &[Self::ColumnData],
        line: u64,
    ) -> Result<()> {
        // Marshall fields into JSON
        self.fields_buffer.clear();
        for (cell, col) in row.into_iter().zip(columns) {
            self.fields_buffer.push_kv(col.as_ref(), cell);
        }
        self.fields_buffer.force_finalize();

        // Execute the insert
        let fields_json = ToSqlOutput::Borrowed(ValueRef::Text(&self.fields_buffer));
        self.insert
            .execute((self.file_name, fields_json, line as u32))?;

        Ok(())
    }
}

impl<'a> ExtraInserter<'a> {
    fn compile_insert(db: &'a rusqlite::Connection) -> Result<rusqlite::Statement<'a>> {
        Ok(db.prepare(
            "INSERT INTO extra_table_rows (table_name,fields_json,row_sort_order) VALUES (?,?,?)",
        )?)
    }
}

/// Builder for a simple `{"key": "value", ...}` JSON string
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GenericFieldsBuilder(Vec<u8>);

impl GenericFieldsBuilder {
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn finalize(&mut self) {
        if !self.is_empty() {
            self.0.push(b'}');
        }
    }

    pub fn force_finalize(&mut self) {
        if self.is_empty() {
            self.0.extend_from_slice(b"{}");
        } else {
            self.0.push(b'}');
        }
    }

    pub fn push_kv(&mut self, k: &[u8], v: &[u8]) {
        self.0.push(if self.is_empty() { b'{' } else { b',' });
        self.push_str(k);
        self.0.push(b':');
        self.push_str(v);
    }

    fn push_str(&mut self, value: &[u8]) {
        const HEX_DIGIT: [u8; 16] = [
            b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'A', b'B', b'C', b'D',
            b'E', b'F',
        ];

        self.0.push(b'"');
        for &c in value {
            match c {
                b'"' | b'\\' => {
                    self.0.push(b'\\');
                    self.0.push(c);
                }
                b'\x08' => self.0.extend_from_slice(b"\\b"),
                b'\x0C' => self.0.extend_from_slice(b"\\f"),
                b'\n' => self.0.extend_from_slice(b"\\n"),
                b'\r' => self.0.extend_from_slice(b"\\r"),
                b'\t' => self.0.extend_from_slice(b"\\t"),
                _ if c.is_ascii_control() => {
                    debug_assert!(c <= 0x1F);
                    self.0.extend_from_slice(b"\\u00");
                    self.0.push(if c >= 0x10 { b'1' } else { b'0' });
                    self.0.push(HEX_DIGIT[(c & 0xF) as usize]);
                }
                _ => self.0.push(c),
            }
        }
        self.0.push(b'"');
    }
}

impl Deref for GenericFieldsBuilder {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::db::register_functions;
    use crate::gtfs::table::{Column, FallbackValue, ParentImplication};

    use super::*;

    #[test]
    fn test_simple() -> Result<()> {
        // Mock a table
        const TABLE: Table = Table {
            sql_name: "spam",
            gtfs_name: "spam.txt",
            columns: &[
                Column::new("foo"),
                Column::with_fallback("bar", FallbackValue::Int(0)),
                Column::new("baz"),
            ],
            required: false,
            parent_implication: None,
            has_extra_fields_json: false,
            filter_clause: "",
            order_clause: "",
        };

        // Mock the database
        let db = rusqlite::Connection::open_in_memory()?;
        register_functions(&db)?;
        db.execute(
            "CREATE TABLE spam (foo TEXT PRIMARY KEY, bar INTEGER NOT NULL, baz TEXT NOT NULL DEFAULT '') STRICT",
            (),
        )?;

        // Load mock CSV
        let data = "foo,baz,bar\r\n1,Hello,42\r\n2,World,\r\n";
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(data.as_bytes());
        let inserter = StandardInserter::new(&db, &TABLE, LoadOptions::default())?;
        Loader::new(inserter, reader).load_file(&db)?;

        // Validate inserted data
        let rows = db
            .prepare("SELECT * FROM spam ORDER BY foo ASC")?
            .query_map((), |row| {
                Ok((
                    row.get::<usize, String>(0)?,
                    row.get::<usize, i32>(1)?,
                    row.get::<usize, String>(2)?,
                ))
            })?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(rows.len(), 2);

        assert_eq!(&rows[0].0, "1");
        assert_eq!(rows[0].1, 42);
        assert_eq!(&rows[0].2, "Hello");

        assert_eq!(&rows[1].0, "2");
        assert_eq!(rows[1].1, 0);
        assert_eq!(&rows[1].2, "World");

        Ok(())
    }

    #[test]
    fn test_parent_implication() -> Result<()> {
        // Mock a table
        const TABLE: Table = Table {
            sql_name: "children",
            gtfs_name: "children.txt",
            columns: &[Column::with_names("p_id", "parent_id"), Column::new("seq")],
            required: false,
            parent_implication: Some(ParentImplication {
                sql_table: "parents",
                sql_column: "p_id",
                gtfs_column: "parent_id",
            }),
            has_extra_fields_json: false,
            filter_clause: "",
            order_clause: "",
        };

        // Mock the database
        let db = rusqlite::Connection::open_in_memory()?;
        register_functions(&db)?;
        db.execute_batch(concat!(
            "PRAGMA foreign_keys = ON;",
            "CREATE TABLE parents (p_id TEXT PRIMARY KEY, attr INTEGER NOT NULL DEFAULT 0) STRICT;",
            "CREATE TABLE children (",
            "  p_id TEXT NOT NULL REFERENCES parents(p_id),",
            "  seq INTEGER NOT NULL CHECK (seq >= 0),",
            "  PRIMARY KEY (p_id, seq)",
            ") STRICT;",
        ))?;

        // Load mock CSV
        let data = "parent_id,seq\r\nA,0\r\nA,1\r\nB,1\r\nB,2\r\n";
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(data.as_bytes());
        let inserter = StandardInserter::new(&db, &TABLE, LoadOptions::default())?;
        Loader::new(inserter, reader).load_file(&db)?;

        // Validate inserted parents
        let parents = db
            .prepare("SELECT * FROM parents ORDER BY p_id ASC")?
            .query_map((), |row| {
                Ok((row.get::<usize, String>(0)?, row.get::<usize, i32>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(parents.len(), 2);

        assert_eq!(&parents[0].0, "A");
        assert_eq!(parents[0].1, 0);

        assert_eq!(&parents[1].0, "B");
        assert_eq!(parents[1].1, 0);

        // Validate inserted children
        let children = db
            .prepare("SELECT * FROM children ORDER BY p_id, seq ASC")?
            .query_map((), |row| {
                Ok((row.get::<usize, String>(0)?, row.get::<usize, i32>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(children.len(), 4);

        assert_eq!(&children[0].0, "A");
        assert_eq!(children[0].1, 0);
        assert_eq!(&children[1].0, "A");
        assert_eq!(children[1].1, 1);

        assert_eq!(&children[2].0, "B");
        assert_eq!(children[2].1, 1);
        assert_eq!(&children[3].0, "B");
        assert_eq!(children[3].1, 2);

        Ok(())
    }

    #[test]
    fn test_extra_fields() -> Result<()> {
        // Mock a table
        const TABLE: Table = Table {
            sql_name: "spam",
            gtfs_name: "spam.txt",
            columns: &[Column::new("foo")],
            required: false,
            parent_implication: None,
            has_extra_fields_json: true,
            filter_clause: "",
            order_clause: "",
        };

        // Mock the database
        let db = rusqlite::Connection::open_in_memory()?;
        register_functions(&db)?;
        db.execute(
            "CREATE TABLE spam (foo TEXT PRIMARY KEY, extra_fields_json TEXT) STRICT",
            (),
        )?;

        // Load mock CSV
        let data = "foo,baz,bar\r\n1,Hello,42\r\n2,World,\r\n";
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(data.as_bytes());
        let options = LoadOptions { extra_fields: true };
        let inserter = StandardInserter::new(&db, &TABLE, options)?;
        Loader::new(inserter, reader).load_file(&db)?;

        // Validate inserted data
        let rows = db
            .prepare(concat!(
                "SELECT foo, json_extract(extra_fields_json, '$.bar'),",
                " json_extract(extra_fields_json, '$.baz') FROM spam ORDER BY foo ASC",
            ))?
            .query_map((), |row| {
                Ok((
                    row.get::<usize, String>(0)?,
                    row.get::<usize, String>(1)?,
                    row.get::<usize, String>(2)?,
                ))
            })?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(rows.len(), 2);

        assert_eq!(&rows[0].0, "1");
        assert_eq!(&rows[0].1, "42");
        assert_eq!(&rows[0].2, "Hello");

        assert_eq!(&rows[1].0, "2");
        assert_eq!(&rows[1].1, "");
        assert_eq!(&rows[1].2, "World");

        Ok(())
    }

    #[test]
    fn test_extra_file() -> Result<()> {
        // Mock the database
        let db = rusqlite::Connection::open_in_memory()?;
        register_functions(&db)?;
        db.execute_batch(concat!(
            "CREATE TABLE extra_table_rows (",
            "  extra_table_row_id INTEGER PRIMARY KEY,",
            "  table_name TEXT NOT NULL,",
            "  fields_json TEXT NOT NULL DEFAULT '{}',",
            "  row_sort_order INTEGER",
            ") STRICT;",
        ))?;

        // Load mock CSV
        let data = "foo,baz,bar\r\n1,Hello,42\r\n2,World,\r\n";
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(data.as_bytes());
        let inserter = ExtraInserter::new(&db, "foo.txt", LoadOptions::default())?;
        Loader::new(inserter, reader).load_file(&db)?;

        // Validate inserted data
        let rows = db
            .prepare("SELECT table_name, fields_json, row_sort_order FROM extra_table_rows ORDER BY row_sort_order ASC")?
            .query_map((), |row| {
                Ok((
                    row.get::<usize, String>(0)?,
                    row.get::<usize, String>(1)?,
                    row.get::<usize, i32>(2)?,
                ))
            })?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(rows.len(), 2);

        assert_eq!(&rows[0].0, "foo.txt");
        assert_eq!(&rows[0].1, r#"{"foo":"1","baz":"Hello","bar":"42"}"#);
        assert_eq!(rows[0].2, 1); // XXX: This should be 2, but https://github.com/BurntSushi/rust-csv/issues/395

        assert_eq!(&rows[1].0, "foo.txt");
        assert_eq!(&rows[1].1, r#"{"foo":"2","baz":"World","bar":""}"#);
        assert_eq!(rows[1].2, 2); // XXX: This should be 3, but https://github.com/BurntSushi/rust-csv/issues/395

        Ok(())
    }

    #[test]
    fn test_generic_fields_builder() {
        let mut b = GenericFieldsBuilder::new();
        b.push_kv(b"Hello", "世界".as_bytes());
        b.push_kv(b"Foo\\Bar\x1FBaz", b"\"Why is this quoted?\"");
        b.finalize();
        assert_eq!(
            str::from_utf8(&b),
            Ok(r#"{"Hello":"世界","Foo\\Bar\u001FBaz":"\"Why is this quoted?\""}"#)
        );

        let mut b = GenericFieldsBuilder::new();
        b.finalize();
        assert_eq!(str::from_utf8(&b), Ok(""));

        let mut b = GenericFieldsBuilder::new();
        b.force_finalize();
        assert_eq!(str::from_utf8(&b), Ok("{}"));
    }
}
