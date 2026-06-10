// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use std::error::Error;
use std::fmt::Display;
use std::path::Path;

/// Error raised when invalid data was provided to custom SQL `parse_xxx` functions, registered
/// by [register_functions].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorInvalidValue(&'static str);

impl Display for ErrorInvalidValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid {}", self.0)
    }
}

impl From<ErrorInvalidValue> for rusqlite::Error {
    fn from(value: ErrorInvalidValue) -> Self {
        rusqlite::Error::UserFunctionError(Box::new(value))
    }
}

impl Error for ErrorInvalidValue {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateText(pub [u8; 10]);

impl rusqlite::ToSql for DateText {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::Borrowed(
            rusqlite::types::ValueRef::Text(&self.0),
        ))
    }
}

/// Opens an SQLite database from the provided path for saving GTFS feeds - optimized for reading.
///
/// [register_functions] is **not** called.
pub fn open_for_save<P: AsRef<Path>>(p: P) -> rusqlite::Result<rusqlite::Connection> {
    use rusqlite::OpenFlags;

    let c = rusqlite::Connection::open_with_flags(
        p,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    // NOTE: no need to register_functions, as those only contain parsing helpers

    c.execute("PRAGMA mmap_size = 4294967296", ())?;
    c.execute("PRAGMA cache_size = -262144", ())?;
    c.execute("PRAGMA temp_store = MEMORY", ())?;
    c.execute("PRAGMA query_only = ON", ())?;

    Ok(c)
}

/// Opens an SQLite database from the provided path for loading GTFS feeds - optimized for writing.
///
/// Reduces the ACID provisions by turning synchronous off and setting the journal_mode
/// and temp_store to in-memory. The database may become garbled on an unexpected power failure,
/// but in that unlikely scenario the operation has to be repeated anyway, so the speed tradeoff
/// is deemed worthwhile.
///
/// [register_functions] is called to set up scalar `parse_gtfs_date`, `parse_gtfs_time` and
/// `parse_gtfs_route_type` functions.
pub fn open_for_load<P: AsRef<Path>>(p: P) -> rusqlite::Result<rusqlite::Connection> {
    use rusqlite::OpenFlags;

    let c = rusqlite::Connection::open_with_flags(
        p,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    register_functions(&c)?;

    c.execute("PRAGMA mmap_size = 2147483648", ())?;
    c.execute("PRAGMA cache_size = -262144", ())?;
    c.execute("PRAGMA synchronous = OFF", ())?;
    c.execute("PRAGMA journal_mode = MEMORY", ())?;
    c.execute("PRAGMA temp_store = MEMORY", ())?;

    // TODO: Disable foreign_keys, and check them in bulk after loading
    c.execute("PRAGMA foreign_keys = ON", ())?;

    Ok(c)
}

/// Registers a couple of functions helpful during GTFS load on the provided connection.
///
/// - `parse_gtfs_date: (str) -> str`: converts a YYYYMMDD date into a YYYY-MM-DD date;
/// - `parse_gtfs_time: (str) -> int`: converts a HH:MM:SS timestamp into total seconds;
/// - `parse_gtfs_route_type: (int) -> int`: converts a possibly extended GTFS route type
///    into its standard equivalent.
pub fn register_functions(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    use rusqlite::functions::FunctionFlags;
    use rusqlite::types::ValueRef;

    db.create_scalar_function(
        "parse_gtfs_date",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let s = match ctx.get_raw(0) {
                ValueRef::Null => return Ok(None),
                ValueRef::Text(s) | ValueRef::Blob(s) => s,
                _ => return Err(ErrorInvalidValue("date - expected TEXT or BLOB").into()),
            };

            if s.len() == 0 {
                Ok(None)
            } else if s.len() == 8 && s.iter().all(|&c| c.is_ascii_digit()) {
                Ok(Some(DateText([
                    s[0], s[1], s[2], s[3], b'-', s[4], s[5], b'-', s[6], s[7],
                ])))
            } else {
                Err(ErrorInvalidValue("date - expected YYYYMMDD").into())
            }
        },
    )?;

    db.create_scalar_function(
        "parse_gtfs_time",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let str = match ctx.get_raw(0) {
                ValueRef::Text(s) | ValueRef::Blob(s) => s,
                _ => return Err(ErrorInvalidValue("time - expected TEXT or BLOB").into()),
            };

            let mut parts = str.split(|&c| c == b':');
            let mut seconds = 0_u32;

            for multiplier in [3600_u32, 60, 1] {
                seconds += parts
                    .next()
                    .and_then(|part| atoi::atoi::<u32>(part))
                    .map(|part| part * multiplier)
                    .ok_or(ErrorInvalidValue("time"))?;
            }

            if parts.next().is_some() {
                Err(ErrorInvalidValue("time - expected HH:MM:SS").into())
            } else {
                Ok(seconds)
            }
        },
    )?;

    db.create_scalar_function(
        "parse_gtfs_route_type",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let i = match ctx.get_raw(0) {
                ValueRef::Integer(i) => i,
                ValueRef::Text(s) | ValueRef::Blob(s) => atoi::atoi(s).ok_or(ErrorInvalidValue(
                    "route_type - TEXT/BLOB does not represent a valid INT",
                ))?,
                _ => {
                    return Err(ErrorInvalidValue("route_type - expected INT, TEXT or BLOB").into());
                }
            };

            match i {
                // Standard types
                0..=7 | 11 | 12 => Ok(i),

                // Extended types
                100..=199 => Ok(2),             // railway service
                200..=299 => Ok(3),             // coach service
                405 => Ok(12),                  // monorail service
                400..=404 | 406..=499 => Ok(1), // urban railway service
                700..=799 => Ok(3),             // bus service
                800..=899 => Ok(11),            // trolleybus service
                900..=999 => Ok(0),             // tram service
                1000..=1199 => Ok(4),           // water service
                1200..=1299 => Ok(4),           // ferry service
                1300..=1399 => Ok(6),           // aerial lift service
                1400..=1499 => Ok(7),           // funicular service

                // Unknown type
                _ => Err(ErrorInvalidValue("route_type").into()),
            }
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::register_functions;
    use rusqlite::types::FromSql;
    use rusqlite::{Connection, Result};

    #[test]
    fn test_register_functions_parse_gtfs_date() -> Result<()> {
        let mut db = Connection::open_in_memory()?;
        register_functions(&mut db)?;

        assert_eq!(
            query_col::<String>(&db, "SELECT parse_gtfs_date('20260315')")?,
            "2026-03-15",
        );

        assert_eq!(
            query_col::<Option<String>>(&db, "SELECT parse_gtfs_date('')")?,
            None,
        );

        assert_eq!(
            query_col::<Option<String>>(&db, "SELECT parse_gtfs_date(NULL)")?,
            None,
        );

        assert!(query_col::<Option<String>>(&db, "SELECT parse_gtfs_date(20260315)").is_err());

        Ok(())
    }

    #[test]
    fn test_register_functions_parse_gtfs_time() -> Result<()> {
        let mut db = Connection::open_in_memory()?;
        register_functions(&mut db)?;

        assert_eq!(
            query_col::<i64>(&db, "SELECT parse_gtfs_time('08:20:30')")?,
            30030,
        );

        assert_eq!(
            query_col::<i64>(&db, "SELECT parse_gtfs_time('26:15:00')")?,
            94500,
        );

        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_time(NULL)").is_err());
        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_time(82030)").is_err());
        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_time('')").is_err());
        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_time('08:20')").is_err());
        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_time('-08:00:00')").is_err());

        Ok(())
    }

    #[test]
    fn test_register_functions_parse_gtfs_route_type() -> Result<()> {
        let mut db = Connection::open_in_memory()?;
        register_functions(&mut db)?;

        assert_eq!(query_col::<i64>(&db, "SELECT parse_gtfs_route_type(0)")?, 0);
        assert_eq!(
            query_col::<i64>(&db, "SELECT parse_gtfs_route_type('800')")?,
            11,
        );

        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_route_type(NULL)").is_err());
        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_route_type(31415)").is_err());
        assert!(query_col::<i64>(&db, "SELECT parse_gtfs_route_type('foo')").is_err());

        Ok(())
    }

    fn query_col<T: FromSql>(db: &Connection, sql: &str) -> Result<T> {
        db.query_one(sql, (), |row| row.get(0))
    }
}
