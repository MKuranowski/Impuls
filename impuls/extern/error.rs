// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use std::backtrace::Backtrace;

#[derive(Debug)]
pub enum ErrorKind {
    Io(std::io::Error),
    Csv(csv::Error),
    SQLite(rusqlite::Error),
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O: {}", e),
            Self::Csv(e) if e.is_io_error() => write!(f, "I/O: {}", e),
            Self::Csv(e) => write!(f, "{}", e),
            Self::SQLite(e) => write!(f, "SQLite: {}", e),
        }
    }
}

/// Any error which can occur during the operation GTFS load or save.
pub struct Error {
    kind: ErrorKind,
    file: Option<String>,
    line: u64,
    backtrace: Backtrace,
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self {
            kind: ErrorKind::Io(value),
            file: None,
            line: 0,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<csv::Error> for Error {
    fn from(value: csv::Error) -> Self {
        let line = value.position().map(|p| p.line()).unwrap_or(0);
        Self {
            kind: ErrorKind::Csv(value),
            file: None,
            line,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Self {
            kind: ErrorKind::SQLite(value),
            file: None,
            line: 0,
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("file", &self.file)
            .field("line", &self.line)
            .finish()?;

        if self.backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            write!(f, "\nBacktrace:\n{}", self.backtrace)?;
        }

        Ok(())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.file {
            Some(s) if self.line != 0 => write!(f, "{}:{}: {}", s, self.line, self.kind),
            Some(s) => write!(f, "{}: {}", s, self.kind),
            None => self.kind.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Io(e) => Some(e),
            ErrorKind::Csv(e) => Some(e),
            ErrorKind::SQLite(e) => Some(e),
        }
    }
}

impl Error {
    pub fn has_io_kind(&self, kind: std::io::ErrorKind) -> bool {
        match &self.kind {
            ErrorKind::Io(e) => e.kind() == kind,
            ErrorKind::Csv(e) => match e.kind() {
                csv::ErrorKind::Io(e) => e.kind() == kind,
                _ => false,
            },
            ErrorKind::SQLite(_) => false,
        }
    }
}

pub trait ResultLocation<T> {
    fn with_file(self, file: impl Into<String>) -> Result<T>;
    fn with_line(self, line: u64) -> Result<T>;
}

impl<T, E: Into<Error>> ResultLocation<T> for std::result::Result<T, E> {
    fn with_file(self, file: impl Into<String>) -> Result<T> {
        self.map_err(|e| {
            let mut e: Error = e.into();
            e.file = Some(file.into());
            e
        })
    }

    fn with_line(self, line: u64) -> Result<T> {
        self.map_err(|e| {
            let mut e: Error = e.into();
            e.line = line;
            e
        })
    }
}

/// Shorthand Result with our [Error].
pub type Result<T, E = Error> = std::result::Result<T, E>;
