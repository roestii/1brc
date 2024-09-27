const std = @import("std");

pub const Record = struct {
    sum: f32,
    cnt: usize,
};

const TableEntry = struct {
    nextEntry: ?*TableEntry,
    k: []u8,
    v: Record,
};

pub const HashTable = struct {
    allocator: std.mem.Allocator,
    buf: [2048]?*TableEntry,

    pub fn init(allocator: std.mem.Allocator) !*HashTable {
        const table = try allocator.create(HashTable);
        table.allocator = allocator;
        table.buf = [_]?*TableEntry{null} ** 2048;
        return table;
    }

    pub fn deinit(self: *HashTable) void {
        for (0..self.buf.len) |i| {
            const r = self.buf[i];
            if (r != null) {
                var current = r.?.nextEntry;
                while (current != null) {
                    const prev = current;
                    current = current.?.nextEntry;
                    self.allocator.destroy(prev.?);
                }
                self.allocator.destroy(r.?);
            }
        }
        self.allocator.destroy(self);
    }

    pub inline fn insert(self: *HashTable, k: []u8, v: Record) !void {
        const i = hash(k);
        const tableEntry = self.buf[i];

        if (tableEntry == null) {
            const entry = try self.allocator.create(TableEntry);
            entry.nextEntry = null;
            entry.k = k;
            entry.v = v;

            self.buf[i] = entry;
        } else {
            if (std.mem.eql(u8, tableEntry.?.k, k)) {
                tableEntry.?.v = v;
            } else {
                var current = tableEntry.?;
                while (current.nextEntry != null) {
                    current = current.nextEntry.?;
                }

                const entry = try self.allocator.create(TableEntry);
                entry.nextEntry = null;
                entry.k = k;
                entry.v = v;

                current.nextEntry = entry;
            }
        }
    }

    pub inline fn get(self: *HashTable, k: []u8) ?*Record {
        const i = hash(k);
        const entry = self.buf[i];

        if (entry == null) {
            return null;
        } else if (std.mem.eql(u8, entry.?.k, k)) {
            return &entry.?.v;
        } else {
            var current = entry.?;
            while (current.nextEntry != null) {
                if (std.mem.eql(u8, current.nextEntry.?.k, k)) {
                    return &current.nextEntry.?.v;
                }
                current = current.nextEntry.?;
            }
        }

        return null;
    }
};

inline fn hash(str: []u8) u11 {
    const asso_values: [256]u16 = [_]u16{
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        110,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        0,
        1269,
        0,
        1269,
        1269,
        1269,
        0,
        0,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        145,
        10,
        240,
        65,
        175,
        33,
        413,
        320,
        280,
        30,
        190,
        325,
        0,
        340,
        335,
        170,
        1269,
        385,
        105,
        115,
        40,
        456,
        190,
        0,
        340,
        0,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        0,
        280,
        280,
        310,
        15,
        15,
        250,
        225,
        5,
        210,
        35,
        25,
        90,
        0,
        30,
        325,
        5,
        5,
        20,
        105,
        200,
        275,
        315,
        5,
        260,
        130,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        0,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        5,
        325,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        20,
        1269,
        1269,
        1269,
        10,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        5,
        10,
        0,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
        1269,
    };

    var hval: usize = str.len;
    switch (str.len) {
        1...2 => {
            hval += asso_values[str[0]];
        },
        3...4 => {
            hval += asso_values[str[0]];
            hval += asso_values[str[2]];
        },
        else => {
            hval += asso_values[str[0]];
            hval += asso_values[str[2]];
            hval += asso_values[str[4]];
        },
    }

    hval += asso_values[str[str.len - 1]];
    return @intCast(hval);
}

test "hashtable insert and get" {
    const h = try HashTable.init(std.testing.allocator);
    defer h.deinit();

    const v = h.get(@constCast("Paris"));
    try std.testing.expect(v == null);

    const r = Record{
        .sum = 0,
        .cnt = 1,
    };

    try h.insert(@constCast("Paris"), r);
    const newV = h.get(@constCast("Paris"));

    try std.testing.expect(newV != null);
    try std.testing.expect(newV.?.sum == 0);
    try std.testing.expect(newV.?.cnt == 1);
}

test "hash" {
    try std.testing.expect(hash(@constCast("Abha")) != hash(@constCast("Naha")));
}
