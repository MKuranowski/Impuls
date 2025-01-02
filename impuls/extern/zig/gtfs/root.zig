// © Copyright 2022-2024 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

const mod_load = @import("load.zig");
pub const load = mod_load.load;

const mod_save = @import("save.zig");
pub const c_char_p = mod_save.c_char_p;
pub const c_char_p_p = mod_save.c_char_p_p;
pub const FileHeader = mod_save.FileHeader;
pub const Headers = mod_save.Headers;
pub const save = mod_save.save;
