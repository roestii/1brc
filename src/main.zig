const std = @import("std");
const hashtable = @import("hashtable.zig");

inline fn splitOnce(input: []u8, comptime del: u8) ?usize {
    if (input.len < 4) {
        for (0..input.len) |i| {
            if (input[i] == del) {
                return i;
            }
        }
    } else {
        var start = input.len - 4;
        while (start >= 0) {
            const c: @Vector(4, u8) = input[start..][0..4].*;
            const dels: @Vector(4, u8) = @splat(@as(u8, del));

            const matches = c == dels;
            const idx = std.simd.firstTrue(matches);

            if (idx != null) {
                const i = idx.? + start;
                return i;
            }

            start -= 4;
        }

        if (start > 0) {
            for (0..start) |i| {
                if (input[i] == del) {
                    return i;
                }
            }
        }
    }

    return null;
}

inline fn updateMap(map: *hashtable.HashTable, line: []u8) !void {
    const split = splitOnce(line, ';').?;
    const val = try std.fmt.parseFloat(f32, line[split + 1 ..]);

    const v = map.get(line[0..split]);
    if (v == null) {
        const entry = hashtable.Record{
            .sum = val,
            .cnt = 1,
        };

        try map.insert(line[0..split], entry);
    } else {
        v.?.*.sum += val;
        v.?.*.cnt += 1;
    }
}

inline fn makeMap(ptr: []u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var map = try hashtable.HashTable.init(allocator);
    defer map.deinit();

    const lookahead = comptime 16;
    var offset: u64 = 0;
    var lastIdx: u64 = 0;

    while (true) {
        if (offset + lookahead > ptr.len) {
            break;
        }

        const input: @Vector(lookahead, u8) = ptr[offset..][0..lookahead].*;
        const newLine: @Vector(lookahead, u8) = @splat(@as(u8, '\n'));
        const matches = input == newLine;
        const count = std.simd.countTrues(matches);

        switch (count) {
            0 => {},
            1 => {
                const idx = std.simd.firstTrue(matches).? + offset;
                try updateMap(map, ptr[lastIdx..idx]);
                lastIdx = idx + 1;
            },
            2 => {
                const firstIdx = std.simd.firstTrue(matches).? + offset;
                const secondIdx = std.simd.lastTrue(matches).? + offset;
                try updateMap(map, ptr[lastIdx..firstIdx]);
                try updateMap(map, ptr[firstIdx + 1 .. secondIdx]);
                lastIdx = secondIdx + 1;
            },
            else => {
                const firstIdx = std.simd.firstTrue(matches).?;
                const secondIdx = std.simd.lastTrue(matches).?;
                try updateMap(map, ptr[lastIdx .. firstIdx + offset]);

                var prev: usize = firstIdx + offset;
                for (firstIdx + 1..secondIdx) |i| {
                    if (matches[i]) {
                        try updateMap(map, ptr[prev + 1 .. i + offset]);
                        prev = i + offset;
                    }
                }

                try updateMap(map, ptr[prev + 1 .. secondIdx + offset]);
                lastIdx = secondIdx + offset + 1;
            },
        }

        offset += lookahead;
    }

    for (offset..ptr.len) |i| {
        if (ptr[i] == '\n') {
            try updateMap(map, ptr[lastIdx..i]);
            lastIdx = i + 1;
        }
    }
}

inline fn parallelMMap(allocator: std.mem.Allocator, ptr: []u8) !void {
    const j = comptime 12;
    const tSize: usize = ptr.len / j;
    var prev: usize = 0;

    var handles = try std.ArrayList(std.Thread).initCapacity(allocator, j);
    defer handles.deinit();

    while (true) {
        if (prev + tSize > ptr.len) {
            break;
        }

        var offset: usize = prev + tSize;
        while (true) {
            const vals: @Vector(16, u8) = ptr[offset..][0..16].*;
            const newLines: @Vector(16, u8) = @splat(@as(u8, '\n'));
            const matches = vals == newLines;

            const idx = std.simd.firstTrue(matches);

            if (idx != null) {
                offset += idx.?;
                break;
            }

            offset += 16;
        }

        const tPtr = ptr[prev .. offset + 1];
        const h = try std.Thread.spawn(.{}, makeMap, .{tPtr});
        try handles.append(h);
        prev = offset + 1;
    }

    if (prev < ptr.len) {
        const h = try std.Thread.spawn(.{}, makeMap, .{ptr[prev..]});
        try handles.append(h);
    }

    for (handles.items) |h| {
        h.join();
    }
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const fName = std.mem.span(std.os.argv[1]);

    const file = try std.fs.cwd().openFile(fName, .{ .mode = .read_write });
    const md = try file.metadata();
    const fileSize = md.size();

    const ptr = try std.posix.mmap(
        null,
        fileSize,
        std.posix.PROT.READ | std.posix.PROT.WRITE,
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );
    defer std.posix.munmap(ptr);
    try parallelMMap(allocator, ptr);
}
