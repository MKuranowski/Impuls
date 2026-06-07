// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

/// Any error which can occur during the operation GTFS load or save.
///
/// [std::io::Error] is folded under [csv::Error], to ensure normalization of error kinds.
#[derive(Debug)]
pub enum Error {
    Csv(csv::Error),
    SQLite(rusqlite::Error),
}

impl From<csv::Error> for Error {
    fn from(value: csv::Error) -> Self {
        Self::Csv(value)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Self::SQLite(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Csv(value.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Csv(e) if e.is_io_error() => write!(f, "io: {}", e),
            Self::Csv(e) => write!(f, "csv: {}", e),
            Self::SQLite(e) => write!(f, "sqlite: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Csv(e) => Some(e),
            Self::SQLite(e) => Some(e),
        }
    }
}

/// Shorthand Result with our [Error].
pub type Result<T, E = Error> = std::result::Result<T, E>;
