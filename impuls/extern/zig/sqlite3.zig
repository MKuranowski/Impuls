// © Copyright 2022-2025 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});
const assert = std.debug.assert;

/// Connection represents a handle to a SQLite database.
pub const Connection = struct {
    handle: *c.sqlite3,

    /// InitOptions represent possible modifiers to the database being opened.
    /// `.{}` can be used as a sensible default for InitOptions.
    ///
    /// - `mode`:
    ///     - `read_write_create` (default): DB is opened for reading and writing,
    ///         creating the DB if it doesn't exist. If the file exists and is read-only,
    ///         the database is opened in read-only mode.
    ///     - `read_write`: DB is opened for reading and writing, raising an error if
    ///         the DB doesn't exist. If the file is read-only,
    ///         the database is opened in read-only mode.
    ///     - `read_only`: DB is opened for reading only, raising an error if the DB doesn' exist.
    /// - `uri` (default: false): the filename argument is interpreted as an URI, instead of
    ///     a literal filename. See <https://www.sqlite.org/c3ref/open.html> for details.
    /// - `memory` (default: false): open a in-memory DB. The filename argument is only used for cache-sharing,
    ///     and is otherwise ignored.
    /// - `threading_mode`:
    ///     - `default`: defer to the start-time or compile-time [threading mode]
    ///     - `no_mutex`: open the DB in "multi-thread" [threading mode] - the database can be accessed
    ///         from multiple threads, as long as Connections, Statements, etc. are **not** shared
    ///         across threads.
    ///     - `full_mutex`: open the DB in "serialized" [threading mode] - the database and
    ///         objects such as Connection, Statements etc. can be shared across threads,
    ///         and SQLite3 will manage serialization to the database, through mutexes.
    /// - `no_follow` (default: false): the database filename is not allowed to be a symlink.
    ///
    /// [threading mode]: https://www.sqlite.org/threadsafe.html
    pub const InitOptions = struct {
        mode: enum {
            read_write_create,
            read_write,
            read_only,
        } = .read_write_create,
        uri: bool = false,
        memory: bool = false,
        threading_mode: enum {
            default,
            no_mutex,
            full_mutex,
        } = .default,
        no_follow: bool = false,

        fn to_c_flags(self: InitOptions) c_int {
            var flags: c_int = 0;
            flags |= switch (self.mode) {
                .read_write_create => c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE,
                .read_write => c.SQLITE_OPEN_READWRITE,
                .read_only => c.SQLITE_OPEN_READONLY,
            };
            flags |= if (self.uri) c.SQLITE_OPEN_URI else 0;
            flags |= if (self.memory) c.SQLITE_OPEN_MEMORY else 0;
            flags |= switch (self.threading_mode) {
                .default => 0,
                .no_mutex => c.SQLITE_OPEN_NOMUTEX,
                .full_mutex => c.SQLITE_OPEN_FULLMUTEX,
            };
            flags |= if (self.no_follow) c.SQLITE_OPEN_NOFOLLOW else 0;
            return flags;
        }
    };

    /// init connects to a database at the provided path.
    ///
    /// Special considerations for the filename parameters:
    /// - if `options.uri` is set to true (default is false), the filename is interpreted as an URI.
    ///     See <https://www.sqlite.org/c3ref/open.html> for details.
    /// - if filename is `""`, a private, on-disk database is opened.
    /// - if filename is `":memory:"`, a private, in-meoery database is opened.
    /// - SQLite revserves the right to treat filenames starting with `:` specially.
    ///     Pass `./:foo` instead of `:foo` to ensure future compatibility.
    ///
    /// See InitOptions for additional details.
    pub fn init(filename: [*:0]const u8, options: InitOptions) !Connection {
        var handle: ?*c.sqlite3 = undefined;
        errdefer _ = c.sqlite3_close(handle);
        try check(c.sqlite3_open_v2(filename, &handle, options.to_c_flags(), null));
        return Connection{ .handle = handle.? };
    }

    /// deinit closes a connection with a database. Asserts that closing
    /// succeeds - all pending Statements, etc. must be also properly closed.
    pub fn deinit(self: Connection) void {
        const code = c.sqlite3_close(self.handle);
        assert(code == c.SQLITE_OK); // sqlite3_close has failed, were all statements deinitialized?
    }

    /// isAutoCommit returns true if a given connection is in the autocommit mode.
    /// This mode is enabled by default, disabled by a BEGIN statement and reenabled
    /// by a COMMIT or ROLLBACK statement.
    pub fn isAutoCommit(self: Connection) bool {
        return c.sqlite3_get_autocommit(self.handle) != 0;
    }

    /// isReadOnly returns true if the main DB of a connection is read only.
    pub fn isReadOnly(self: Connection) bool {
        return c.sqlite3_db_readonly(self.handle, c.sqlite3_db_name(self.handle, 0)) != 0;
    }

    /// errMsg returns an english description of the most recent error, or "" in case
    /// there is no error, or an error description is not available.
    ///
    /// The returned string is valid until the next call to any other SQLite function.
    pub fn errMsg(self: Connection) [*:0]const u8 {
        const code = c.sqlite3_errcode(self.handle);
        if (code == c.SQLITE_OK) {
            return "";
        } else if (c.sqlite3_errmsg(self.handle)) |s| {
            return s;
        } else if (c.sqlite3_errstr(code)) |s| {
            return s;
        } else {
            return "";
        }
    }

    /// preapre compiles single SQL statement into byte-code that queries or updates the database.
    /// SQL Literals can be replaced by placeholders (usually "?"), which can
    /// be dynamically bound to specific values using Statement.bind or Statement.bindAll.
    ///
    /// Asserts that the entirety of `sql` was compiled - which failes if `sql` contains multiple
    /// statements.
    pub fn prepare(self: Connection, sql: [*:0]const u8) !Statement {
        var handle: ?*c.sqlite3_stmt = undefined;
        var rest: [*:0]const u8 = undefined;
        errdefer _ = c.sqlite3_finalize(handle);
        try check(c.sqlite3_prepare_v2(
            self.handle,
            sql,
            -1,
            &handle,
            @ptrCast(&rest),
        ));
        assert(rest[0] == 0); // SQLite did not compile the entirety of `sql`
        return Statement{ .handle = handle.? };
    }

    /// exec compiles a single SQL statement and executes SQL it, without binding any parameters.
    /// Equivalent to prepare-exec-deinit.
    pub fn exec(self: Connection, sql: [*:0]const u8) !void {
        var stmt = try self.prepare(sql);
        defer stmt.deinit();
        try stmt.exec();
    }

    /// execWithArgs compiles single SQL statement SQL, binds provided parameters
    /// and executes the statement. Equivalent to preapre-execWithArgs-deinit.
    ///
    /// See the documentation for Statement.bind for restrictions on the arguments
    /// passed through the tuple. Lifetime restrictions for string arguments don't apply.
    pub fn execWithArgs(self: Connection, sql: [*:0]const u8, argsTuple: anytype) !void {
        var stmt = try self.prepare(sql);
        defer stmt.deinit();
        try stmt.execWithArgs(argsTuple);
    }

    /// execMany compiles and executes multiple SQL statements.
    /// Equivalent to calling exec() on each semicolon-separated part of sql_script.
    pub fn execMany(self: Connection, sql_script: [*:0]const u8) !void {
        var script_to_process: [*:0]const u8 = sql_script;
        while (script_to_process[0] != 0) {
            var handle: ?*c.sqlite3_stmt = undefined;
            var rest: [*:0]const u8 = undefined;

            errdefer _ = c.sqlite3_finalize(handle);
            try check(c.sqlite3_prepare_v2(
                self.handle,
                script_to_process,
                -1,
                &handle,
                @ptrCast(&rest),
            ));
            script_to_process = rest;
            if (handle == null) continue;

            var stmt = Statement{ .handle = handle.? };
            try stmt.exec();
            stmt.deinit();
        }
    }
};

/// Statement represents a single, compiled SQL statement.
pub const Statement = struct {
    handle: *c.sqlite3_stmt,

    /// deinit releases resources used by the statement.
    ///
    /// If not called, Connection.deinit will fail.
    pub fn deinit(self: Statement) void {
        const code = c.sqlite3_finalize(self.handle);
        assert(code == c.SQLITE_OK); // sqlite3_finalize has failed
    }

    /// reset brings a statement back to its initial state, ready to be re-executed.
    /// Variable bindings are **not** cleared, use clearBindings.
    pub fn reset(self: Statement) !void {
        try check(c.sqlite3_reset(self.handle));
    }

    /// clearBindings resets all bound parameters to `NULL`.
    pub fn clearBindings(self: Statement) !void {
        try check(c.sqlite3_clear_bindings(self.handle));
    }

    /// bindParameterCount returns the number of parameter in the compiled statement.
    ///
    /// Unless "?NNN" placeholders are used, this is the index of
    /// the rightmost placeholder.
    pub fn bindParameterCount(self: Statement) c_int {
        return c.sqlite3_bind_parameter_count(self.handle);
    }

    /// bindParameterIndex finds the index of a parameter with a given name,
    /// or 0 if no match was found. Leftmost parameter has index 1.
    pub fn bindParameterIndex(self: Statement, name: [*:0]const u8) c_int {
        return c.sqlite3_bind_parameter_index(self.handle, name);
    }

    /// bindParameterName finds the name of a parameter with a given index.
    /// Leading "?", ":" or "@" is considered part of the name.
    ///
    /// Returns null if index is out-of-bounds or the argument is anonymous (simple "?").
    ///
    /// The returned string is invalidated once the Statement is deinitialized.
    pub fn bindParameterName(self: Statement, oneBasedIndex: c_int) ?[*:0]const u8 {
        return c.sqlite3_bind_parameter_name(self.handle, oneBasedIndex);
    }

    /// bind binds a concrete value to a parameter of the prepared statement.
    /// The argument may be of the following types:
    /// - int, which will be implicitly casted to sqlite_int64 (usually c_longlong);
    /// - float, which will be implicitly casted to f64;
    /// - bool, with false mapped to 0 and true to 1;
    /// - null, which causes SQL NULL to be bound to the placeholder;
    /// - []u8, *[N]u8, or [*:0]u8, which is assumed to be UTF-8 encoded text;
    /// - []u16, *[N]u16, or [*:0]u16, which is assumed to be UTF-16 encoded text;
    /// - an optional of any of the above types.
    ///
    /// Text arguments ([]u8/[]u16/[\*:0]u8/[\*:0]u16) must remain
    /// valid and unchanged until the next call to:
    /// 1. bind with the same `oneBasedIndex`,
    /// 2. bindAll,
    /// 3. clearBindings,
    /// 4. execWithArgs, or
    /// 5. deinit.
    /// Note that `stepUntilDone`, `exec` and `reset` **do not alleviate the lifetime requirement**,
    /// as they do not affect parameter binding.
    ///
    /// Text arguments can also be `const`.
    ///
    /// The leftmost parameter has index 1.
    pub fn bind(self: Statement, oneBasedIndex: c_int, arg: anytype) !void {
        switch (@typeInfo(@TypeOf(arg))) {
            .int, .comptime_int => {
                try check(c.sqlite3_bind_int64(self.handle, oneBasedIndex, @intCast(arg)));
            },

            .float, .comptime_float => {
                try check(c.sqlite3_bind_double(self.handle, oneBasedIndex, @floatCast(arg)));
            },

            .bool => {
                try check(c.sqlite3_bind_int(self.handle, oneBasedIndex, @intFromBool(arg)));
            },

            .null => {
                try check(c.sqlite3_bind_null(self.handle, oneBasedIndex));
            },

            .optional => {
                if (arg) |value| {
                    return self.bind(oneBasedIndex, value);
                } else {
                    return self.bind(oneBasedIndex, null);
                }
            },

            .pointer => |ptr| {
                if (ptr.child == u8 and ptr.size == .slice) {
                    // Pass []u8 as UTF-8 TEXT, with explicit size
                    try check(c.sqlite3_bind_text64(
                        self.handle,
                        oneBasedIndex,
                        @ptrCast(arg),
                        @intCast(arg.len),
                        c.SQLITE_STATIC,
                        c.SQLITE_UTF8,
                    ));
                } else if (ptr.child == u16 and ptr.size == .slice) {
                    // Pass []u8 as UTF-16 TEXT, with explicit size
                    try check(c.sqlite3_bind_text64(
                        self.handle,
                        oneBasedIndex,
                        arg,
                        @intCast(arg.len),
                        c.SQLITE_STATIC,
                        c.SQLITE_UTF16,
                    ));
                } else if (comptime is_ptr_to_null_terminated(ptr, u8)) {
                    // Pass [*:0]u8 as UTF-8 TEXT, letting sqlite find the sentinel null terminator
                    try check(c.sqlite3_bind_text(
                        self.handle,
                        oneBasedIndex,
                        arg,
                        -1,
                        c.SQLITE_STATIC,
                    ));
                } else if (comptime is_ptr_to_null_terminated(ptr, u16)) {
                    // Pass [*:0]u16 as UTF-16 TEXT,
                    // letting sqlite find the sentinel null terminator
                    try check(c.sqlite3_bind_text16(
                        self.handle,
                        oneBasedIndex,
                        arg,
                        -1,
                        c.SQLITE_STATIC,
                    ));
                } else if (comptime is_ptr_to_array_of(ptr, u8)) |len| {
                    // Pass *[len]u8 as UTF-8 TEXT, with explicit size
                    try check(c.sqlite3_bind_text64(
                        self.handle,
                        oneBasedIndex,
                        arg,
                        @intCast(len),
                        c.SQLITE_STATIC,
                        c.SQLITE_UTF8,
                    ));
                } else if (comptime is_ptr_to_array_of(ptr, u16)) |len| {
                    // Pass *[len]u16 as UTF-16 TEXT, with explicit size
                    try check(c.sqlite3_bind_text64(
                        self.handle,
                        oneBasedIndex,
                        arg,
                        @intCast(len),
                        c.SQLITE_STATIC,
                        c.SQLITE_UTF16,
                    ));
                } else {
                    @compileError("unable to bind type " ++ @typeName(@TypeOf(arg)) ++ " (child: " ++ @typeName(ptr.child) ++ ")");
                }
            },

            else => @compileError("unable to bind type " ++ @typeName(@TypeOf(arg))),
        }
    }

    /// bindAll binds all values from the provided tuple.
    ///
    /// Asserts that the tuple has the same number of fields as there are parameters.
    ///
    /// See the documentation for Statement.bind for restrictions on the arguments
    /// passed through the tuple, especially for lifetime requirements for string types.
    pub fn bindAll(self: Statement, argsTuple: anytype) !void {
        // Check that argsTuple is a tuple of the required size
        switch (@typeInfo(@TypeOf(argsTuple))) {
            .@"struct" => |s| {
                if (!s.is_tuple) {
                    @compileError("argsTuple is not a tuple");
                }

                assert(s.fields.len == self.bindParameterCount()); // argsTuple should have as many fields as there are query params
            },
            else => @compileError("argsTuple is not a tuple"),
        }

        inline for (argsTuple, 1..) |arg, i| {
            try self.bind(i, arg);
        }
    }

    /// step advances execution of a query. Returns true if a row is avalilable,
    /// false if execution has finished, or an error.
    pub fn step(self: Statement) Error!bool {
        switch (c.sqlite3_step(self.handle)) {
            c.SQLITE_ROW => return true,
            c.SQLITE_DONE => return false,
            else => |code| return codeToErr(code),
        }
    }

    /// stepUntilDone calls step() until query has finished executing.
    pub fn stepUntilDone(self: Statement) !void {
        while (try self.step()) {}
    }

    /// columnCount returns the number of columns in the result row.
    pub fn columnCount(self: Statement) c_int {
        return c.sqlite3_column_count(self.handle);
    }

    /// columnType returns the **current** datatype of the provded column.
    ///
    /// The type my change with a call to column or columns.
    pub fn columnType(self: Statement, zeroBasedIndex: c_int) Datatype {
        return @enumFromInt(c.sqlite3_column_type(self.handle, zeroBasedIndex));
    }

    /// column reads the value of a column of the current row to a provided pointer.
    ///
    /// `ptr` must be a pointer to one of the following types:
    /// - bool;
    /// - int;
    /// - float;
    /// - []const u8, [:0]const u8, [\*:0]const u8, []const u16, [:0]const u16 or
    ///     [\*:0]const u16 (referred further as "text");
    /// - optional to any of the following values.
    ///
    /// `ptr` can also be a literal null, in which case the call is a no-op.
    ///
    /// SQLite doesn't have a boolean type; all conversions go through int.
    /// If size of the provided int/float is smaller than 64-bits, the result may be truncated.
    ///
    /// Text values are borrowed from the SQLite engine (and thus they can't be modified)
    /// and remain valid until the next call to:
    /// 1. column with the same `zeroBasedIndex`,
    /// 2. columns,
    /// 3. reset,
    /// 4. step,
    /// 5. exec,
    /// 6. execWithArgs, or
    /// 7. deinit.
    ///
    /// Note that SQLite is dynamically typed, and will perform automatic type conversion:
    /// | SQLite Type | Requested (Zig) Type | Conversion                           |
    /// |-------------|----------------------|--------------------------------------|
    /// | NULL        | bool                 | `false`                              |
    /// | NULL        | int                  | `0`                                  |
    /// | NULL        | float                | `0.0`                                |
    /// | NULL        | text                 | empty string                         |
    /// | INTEGER     | bool                 | if i == 0 { false } else { true }    |
    /// | INTEGER     | float                | @floatFromInt                        |
    /// | INTEGER     | text                 | ASCII representation                 |
    /// | FLOAT       | bool                 | First to `int`, then to `bool`.      |
    /// | FLOAT       | int                  | As per SQLite's `CAST(i as INTEGER)` |
    /// | FLOAT       | text                 | ASCII representation                 |
    /// | TEXT        | bool                 | First to `int`, then to `bool`       |
    /// | TEXT        | int                  | As per SQLite's `CAST(i AS INTEGER)` |
    /// | TEXT        | float                | As per SQLite's `CAST(i AS REAL)`    |
    /// | BLOB        | bool                 | First to `int`, then to `bool`       |
    /// | BLOB        | int                  | As per SQLite's `CAST(i AS INTEGER)` |
    /// | BLOB        | float                | As per SQLite's `CAST(i AS REAL)`    |
    /// | BLOB        | TEXT                 | As per SQLite's `CAST(i AS TEXT)`    |
    ///
    /// The caller shouldn't attempt to read the same column into two different types.
    /// Once a read happens, SQLite assumes the value is supposed to have the type of the
    /// first call. For example, scanning `NULL` into a i32, causes `0` to be saved; but, all
    /// further calls will return `0` (even if scanned into a ?i32). The only loseless scan is
    /// to an optional text (e.g. ?[]u8).
    ///
    /// Those conversions are performed automatically by SQLite, see https://www.sqlite.org/c3ref/column_blob.html.
    /// For details on how CAST works, see https://www.sqlite.org/lang_expr.html#castexpr.
    pub fn column(self: Statement, zeroBasedIndex: c_int, ptr: anytype) void {
        const T = switch (@typeInfo(@TypeOf(ptr))) {
            .pointer => |p| if (p.size != .one or p.is_const)
                @compileError("unable to save column to " ++ @typeName(@TypeOf(ptr)) ++ ", argument must be a non-const pointer")
            else
                p.child,
            .null => return,
            else => @compileError("unable to save column to " ++ @typeName(@TypeOf(ptr)) ++ ", argument must be a non-const pointer"),
        };

        assert(zeroBasedIndex < self.columnCount()); // column index out-of-bounds

        switch (@typeInfo(T)) {
            .bool => {
                if (c.sqlite3_column_int(self.handle, zeroBasedIndex) == 0) {
                    ptr.* = false;
                } else {
                    ptr.* = true;
                }
            },

            .int => {
                ptr.* = @intCast(c.sqlite3_column_int64(self.handle, zeroBasedIndex));
            },

            .float => {
                ptr.* = @floatCast(c.sqlite3_column_double(self.handle, zeroBasedIndex));
            },

            .optional => |o| {
                if (self.columnType(zeroBasedIndex) == .Null) {
                    ptr.* = null;
                } else {
                    var concrete: o.child = undefined;
                    self.column(zeroBasedIndex, &concrete);
                    ptr.* = concrete;
                }
            },

            .pointer => |p| {
                if (p.child == u8 and p.size == .slice and p.sentinel() == null and p.is_const) {
                    const str = c.sqlite3_column_text(self.handle, zeroBasedIndex);
                    const len = c.sqlite3_column_bytes(self.handle, zeroBasedIndex);
                    ptr.* = str[0..@intCast(len)];
                } else if (p.child == u8 and p.size == .slice and p.sentinel() == 0 and p.is_const) {
                    const str = c.sqlite3_column_text(self.handle, zeroBasedIndex);
                    const len = c.sqlite3_column_bytes(self.handle, zeroBasedIndex);
                    ptr.* = str[0..@intCast(len) :0];
                } else if (p.child == u16 and p.size == .slice and p.sentinel() == null and p.is_const) {
                    const str = c.sqlite3_column_text16(self.handle, zeroBasedIndex);
                    const len = c.sqlite3_column_bytes16(self.handle, zeroBasedIndex);
                    ptr.* = str[0..@intCast(len)];
                } else if (p.child == u16 and p.size == .slice and p.sentinel() == 0 and p.is_const) {
                    const str = c.sqlite3_column_text16(self.handle, zeroBasedIndex);
                    const len = c.sqlite3_column_bytes16(self.handle, zeroBasedIndex);
                    ptr.* = str[0..@intCast(len) :0];
                } else if (p.child == u8 and p.size == .many and p.sentinel() == 0 and p.is_const) {
                    ptr.* = c.sqlite3_column_text(self.handle, zeroBasedIndex);
                } else if (p.child == u16 and p.size == .many and p.sentinel() == 0 and p.is_const) {
                    ptr.* = c.sqlite3_column_text16(self.handle, zeroBasedIndex);
                } else {
                    @compileError("unable to read column to type " ++ @typeName(@TypeOf(T)));
                }
            },

            else => @compileError("unable to read column to type " ++ @typeName(@TypeOf(T))),
        }
    }

    /// columns reads the value of the current row to a tuple of pointers.
    /// Asserts that `tupleOfPointers` has the same number of fields as there are columns in
    /// the result. Elements can also be literal `null`s, which skip a given column.
    ///
    /// See the documentation of `column` for details on allowed types and related
    /// caveats, especially lifetime restrictions on strings.
    pub fn columns(self: Statement, tupleOfPointers: anytype) void {
        switch (@typeInfo(@TypeOf(tupleOfPointers))) {
            .@"struct" => |s| {
                if (!s.is_tuple) {
                    @compileError("tupleOfPointers is not a tuple");
                }

                assert(s.fields.len == self.columnCount()); // tupleOfPointer must have as many fields as there are columns
            },
            else => @compileError("tupleOfPointers is not a tuple"),
        }

        inline for (tupleOfPointers, 0..) |arg, i| {
            self.column(i, arg);
        }
    }

    /// exec resets the statement and steps the execution until done.
    /// Parameter bindings are retained.
    pub fn exec(self: Statement) !void {
        try self.reset();
        try self.stepUntilDone();
    }

    /// execWithArgs rebinds all arguments, resets the statement and steps the execution until done.
    /// Equivalent to bindAll-exec.
    ///
    /// Asserts that the tuple has the same number of fields as there are parameters.
    ///
    /// See the documentation for Statement.bind for restrictions on the arguments
    /// passed through the tuple, especially for lifetime requirements for string types.
    pub fn execWithArgs(self: Statement, argsTuple: anytype) !void {
        try self.reset();
        try self.bindAll(argsTuple);
        try self.stepUntilDone();
    }

    /// errMsg returns an english description of the most recent error of the underlying connection,
    /// or "" in case there is no error, or an error description is not available.
    ///
    /// The returned string is valid until the next call to any other SQLite function, even if
    /// done through another Statement instance.
    pub fn errMsg(self: Statement) [*:0]const u8 {
        const conn_handle = c.sqlite3_db_handle(self.handle);
        const code = c.sqlite3_errcode(conn_handle);
        if (code == c.SQLITE_OK) {
            return "";
        } else if (c.sqlite3_errmsg(conn_handle)) |s| {
            return s;
        } else if (c.sqlite3_errstr(code)) |s| {
            return s;
        } else {
            return "";
        }
    }
};

/// Error represents errors returned by SQLite.
pub const Error = error{
    /// Operation was aborted prior to completion, usually be application request.
    SQLiteAbort,

    /// Authorizer callback indicates that an SQL statement being prepared is not authorized.
    SQLiteAuth,

    /// Database file could not be written (or in some cases read) because of concurrent activity
    /// by some other database connection, usually a database connection in a separate process.
    SQLiteBusy,

    /// Unable to open a file.
    SQLiteCantOpen,

    /// SQL constraint violation occurred while trying to process an SQL statement.
    /// Connection.errMsg will likely contain a more detailed explanation.
    SQLiteConstraint,

    /// Database file has been corrupted.
    SQLiteCorrupt,

    /// Not currently used.
    SQLiteEmpty,

    /// Generic error code, used when no other more specific error code is available.
    SQLiteError,

    /// Not currently used.
    SQLiteFormat,

    /// Write could not complete because the disk is full.
    SQLiteFull,

    /// Internal malfunction. In a working version of SQLite, an application should
    /// never see this result code. If application does encounter this result code, it shows that
    /// there is a bug in the database engine.
    SQLiteInternal,

    /// Operation was interrupted by the SQLite's interrupt interface.
    SQLiteInterrupt,

    /// Operation could not finish because the operating system reported an I/O error.
    SQLiteIoErr,

    /// Write operation could not continue because of a conflict within the same database connection
    /// or a conflict with a different database connection that uses a shared cache.
    SQLiteLocked,

    /// Indicates a datatype mismatch.
    SQLiteMismatch,

    /// Application uses any SQLite interface in a way that is undefined or unsupported.
    /// This means that the application is incorrectly coded and needs to be fixed.
    SQLiteMisuse,

    /// Database grows to be larger than what the filesystem can handle.
    SQLiteNoLFS,

    /// SQLite was unable to allocate all the memory it needed to complete the operation.
    SQLiteNoMem,

    /// File being opened does not appear to be an SQLite database file.
    SQLiteNotADb,

    /// 1. File control opcode was not recognized by the underlying VFS;
    /// 2. Returned by VFS's xSetSystemCall method; or
    /// 3. RHS constraint not available for a virtual table's xBestIndex method call.
    SQLiteNotFound,

    /// Requested access mode for a newly created database could not be provided.
    SQLitePerm,

    /// Indicates a problem with the file locking protocol used by SQLite.
    SQLiteProtocol,

    /// Parameter or column index is out of range.
    SQLiteRange,

    /// Attempt is made to alter some data for which the current database connection does not
    /// have write permission.
    SQLiteReadOnly,

    /// Indicates that the database schema has changed, and a statement needs to be recompiled.
    SQLiteSchema,

    /// A string or BLOB was too large.
    SQLiteTooBig,
};

/// isSQLiteError returns true if the provided error represents an SQLite issue.
pub fn isSQLiteError(err: anyerror) bool {
    return switch (err) {
        Error.SQLiteAbort => true,
        Error.SQLiteAuth => true,
        Error.SQLiteBusy => true,
        Error.SQLiteCantOpen => true,
        Error.SQLiteConstraint => true,
        Error.SQLiteCorrupt => true,
        Error.SQLiteEmpty => true,
        Error.SQLiteError => true,
        Error.SQLiteFormat => true,
        Error.SQLiteFull => true,
        Error.SQLiteInternal => true,
        Error.SQLiteInterrupt => true,
        Error.SQLiteIoErr => true,
        Error.SQLiteLocked => true,
        Error.SQLiteMismatch => true,
        Error.SQLiteMisuse => true,
        Error.SQLiteNoLFS => true,
        Error.SQLiteNoMem => true,
        Error.SQLiteNotADb => true,
        Error.SQLiteNotFound => true,
        Error.SQLitePerm => true,
        Error.SQLiteProtocol => true,
        Error.SQLiteRange => true,
        Error.SQLiteReadOnly => true,
        Error.SQLiteSchema => true,
        Error.SQLiteTooBig => true,
        else => false,
    };
}

fn codeToErr(code: c_int) Error {
    return switch (code) {
        c.SQLITE_ABORT => Error.SQLiteAbort,
        c.SQLITE_AUTH => Error.SQLiteAuth,
        c.SQLITE_BUSY => Error.SQLiteBusy,
        c.SQLITE_CANTOPEN => Error.SQLiteCantOpen,
        c.SQLITE_CONSTRAINT => Error.SQLiteConstraint,
        c.SQLITE_CORRUPT => Error.SQLiteCorrupt,
        c.SQLITE_EMPTY => Error.SQLiteEmpty,
        c.SQLITE_FORMAT => Error.SQLiteFormat,
        c.SQLITE_FULL => Error.SQLiteFull,
        c.SQLITE_INTERNAL => Error.SQLiteInternal,
        c.SQLITE_INTERRUPT => Error.SQLiteInterrupt,
        c.SQLITE_IOERR => Error.SQLiteIoErr,
        c.SQLITE_LOCKED => Error.SQLiteLocked,
        c.SQLITE_MISMATCH => Error.SQLiteMismatch,
        c.SQLITE_MISUSE => Error.SQLiteMisuse,
        c.SQLITE_NOLFS => Error.SQLiteNoLFS,
        c.SQLITE_NOMEM => Error.SQLiteNoMem,
        c.SQLITE_NOTADB => Error.SQLiteNotADb,
        c.SQLITE_NOTFOUND => Error.SQLiteNotFound,
        c.SQLITE_PERM => Error.SQLitePerm,
        c.SQLITE_PROTOCOL => Error.SQLiteProtocol,
        c.SQLITE_RANGE => Error.SQLiteRange,
        c.SQLITE_READONLY => Error.SQLiteReadOnly,
        c.SQLITE_SCHEMA => Error.SQLiteSchema,
        c.SQLITE_TOOBIG => Error.SQLiteTooBig,
        else => Error.SQLiteError,
    };
}

fn check(code: c_int) Error!void {
    return switch (code) {
        c.SQLITE_OK, c.SQLITE_ROW, c.SQLITE_DONE => {},
        else => codeToErr(code),
    };
}

/// Datatype represents the fundamental type of an SQLite type.
pub const Datatype = enum(u3) {
    Integer = c.SQLITE_INTEGER,
    Float = c.SQLITE_FLOAT,
    Text = c.SQLITE_TEXT,
    Blob = c.SQLITE_BLOB,
    Null = c.SQLITE_NULL,
};

test "sqlite.basic" {
    var db = try Connection.init(":memory:", .{});
    defer db.deinit();

    try db.exec("CREATE TABLE cities(name TEXT, ascii_name TEXT, country TEXT, population INTEGER)");

    {
        // Source (2024-03-01): https://en.wikipedia.org/wiki/List_of_largest_cities

        var insert = try db.prepare("INSERT INTO cities (name, ascii_name, country, population) VALUES (?, ?, ?, ?)");
        defer insert.deinit();
        try db.exec("BEGIN");
        try insert.execWithArgs(.{ "東京", "Tokyo", "JP", 13_515_271 });
        try insert.execWithArgs(.{ "São Paulo", "Sao Paulo", "BR", 12_252_023 });
        try insert.execWithArgs(.{ "Mexico City", null, "MX", 9_209_944 });
        try insert.execWithArgs(.{ "Cairo", null, "EG", 10_044_894 });
        try insert.execWithArgs(.{ "London", null, "UK", 8_825_001 });
        try db.exec("COMMIT");
    }

    {
        var select = try db.prepare("SELECT * FROM cities WHERE population > 10000000 ORDER BY population DESC");
        defer select.deinit();
        try std.testing.expectEqual(@as(c_int, 4), select.columnCount());

        var name: []const u8 = undefined;
        var ascii_name: ?[]const u8 = undefined;
        var country: []const u8 = undefined;
        var population: u64 = undefined;

        try std.testing.expect(try select.step());
        select.columns(.{ &name, &ascii_name, &country, &population });
        try std.testing.expectEqualStrings("東京", name);
        try std.testing.expectEqualStrings("Tokyo", ascii_name.?);
        try std.testing.expectEqualStrings("JP", country);
        try std.testing.expectEqual(@as(u64, 13_515_271), population);

        try std.testing.expect(try select.step());
        select.columns(.{ &name, &ascii_name, &country, &population });
        try std.testing.expectEqualStrings("São Paulo", name);
        try std.testing.expectEqualStrings("Sao Paulo", ascii_name.?);
        try std.testing.expectEqualStrings("BR", country);
        try std.testing.expectEqual(@as(u64, 12_252_023), population);

        try std.testing.expect(try select.step());
        select.columns(.{ &name, &ascii_name, &country, &population });
        try std.testing.expectEqualStrings("Cairo", name);
        try std.testing.expect(ascii_name == null);
        try std.testing.expectEqualStrings("EG", country);
        try std.testing.expectEqual(@as(u64, 10_044_894), population);

        try std.testing.expect(!try select.step());
    }
}

/// is_ptr_to_array_of returns N if ptr represents `*[N]child` or null otherwise.
fn is_ptr_to_array_of(comptime ptr: std.builtin.Type.Pointer, comptime child: type) ?comptime_int {
    return if (ptr.size == .one) switch (@typeInfo(ptr.child)) {
        .array => |arr| if (arr.child == child) arr.len else null,
        else => null,
    } else null;
}

/// is_ptr_to_array_of returns true if ptr represents `[*:0]child`.
///
/// `comptime_int` must be able to be coerced into `child`, otherwise the null terminator check
/// will fail to compile.
fn is_ptr_to_null_terminated(comptime ptr: std.builtin.Type.Pointer, comptime child: type) bool {
    return ptr.child == child and ptr.size == .many and ptr.sentinel() == 0;
}
