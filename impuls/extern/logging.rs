// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use std::ffi::{CString, c_char, c_int};
use std::sync::{Mutex, Once};

/// Log handler type - C-compatible function called with every message, with
/// [Python-style level](https://docs.python.org/3/library/logging.html#logging-levels)
/// and a pointer to the formatted string.
pub type LogHandler = extern "C" fn(level: c_int, message: *const c_char);

/// Global logger instance
static GLOBAL_LOGGER: Logger = Logger::new(None);

/// One-time lock ensuring [GLOBAL_LOGGER] is hooked into [log] exactly once.
/// Note that this does not prevent changing the active [LogHandler].
static GLOBAL_LOGGER_INSTALL: Once = Once::new();

/// [log]-compatible wrapper around [LogHandler]
#[derive(Debug, Default)]
pub struct Logger(Mutex<Option<LogHandler>>);

impl Logger {
    /// Instantiates a new Logger with the provided handler
    pub const fn new(handler: Option<LogHandler>) -> Self {
        Self(Mutex::new(handler))
    }

    /// Converts a [log::Level] into a [Python-style level](https://docs.python.org/3/library/logging.html#logging-levels)
    pub const fn level_as_int(l: log::Level) -> c_int {
        match l {
            log::Level::Error => 40,
            log::Level::Warn => 30,
            log::Level::Info => 20,
            log::Level::Debug => 10,
            log::Level::Trace => 5,
        }
    }

    /// Updates the inner [LogHandler] used for logging calls.
    pub fn set_handler(&self, handler: Option<LogHandler>) {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        *guard = handler;
        self.0.clear_poison();
    }
}

impl log::Log for Logger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            unsafe {
                let level = Self::level_as_int(record.level());
                let msg = CString::from_vec_unchecked(format!("{}", record.args()).into_bytes());

                self.0
                    .lock()
                    .expect("log mutex should not be poisoned")
                    .map(|handler| handler(level, msg.as_ptr()));
            }
        }
    }

    fn flush(&self) {}
}

/// Sets the global log handler to the provided function.
/// Installs the [GLOBAL_LOGGER] into the [log] module on the first call.
pub fn set_global_handler(handler: Option<LogHandler>) {
    GLOBAL_LOGGER.set_handler(handler);
    GLOBAL_LOGGER_INSTALL.call_once(|| {
        log::set_logger(&GLOBAL_LOGGER).expect("log initialization should not fail");
    });
}
