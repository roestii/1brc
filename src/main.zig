const std = @import("std");

const SplitOnce = struct { first: []u8, second: []u8 };

fn splitOnce(input: []u8, comptime del: u8) ?SplitOnce {
    if (input.len < 4) {
        for (0..input.len) |i| {
            if (input[i] == del) {
                return .{ .first = input[0..i], .second = input[i + 1 ..] };
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
                return .{ .first = input[0..i], .second = input[i + 1 ..] };
            }

            start -= 4;
        }

        if (start > 0) {
            for (0..start) |i| {
                if (input[i] == del) {
                    return .{ .first = input[0..i], .second = input[i + 1 ..] };
                }
            }
        }
    }

    return null;
}

fn updateMap(map: *std.StringHashMap(f32), line: []u8) !void {
    const split = splitOnce(line, ';').?;
    const val = try std.fmt.parseFloat(f32, split.second);

    const v = map.getPtr(split.first);
    if (v == null) {
        try map.put(split.first, val);
    } else {
        v.?.* += val;
    }
}

fn makeMap(ptr: []u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var map = std.StringHashMap(f32).init(allocator);
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
                try updateMap(&map, ptr[lastIdx + 1 .. idx]);
                lastIdx = idx;
            },
            2 => {
                const firstIdx = std.simd.firstTrue(matches).? + offset;
                const secondIdx = std.simd.lastTrue(matches).? + offset;
                try updateMap(&map, ptr[lastIdx + 1 .. firstIdx]);
                try updateMap(&map, ptr[firstIdx + 1 .. secondIdx]);
                lastIdx = secondIdx;
            },
            else => {
                const firstIdx = std.simd.firstTrue(matches).?;
                const secondIdx = std.simd.lastTrue(matches).?;
                try updateMap(&map, ptr[lastIdx + 1 .. firstIdx + offset]);

                var prev: usize = firstIdx + offset;
                for (firstIdx + 1..secondIdx) |i| {
                    if (matches[i]) {
                        try updateMap(&map, ptr[prev + 1 .. i + offset]);
                        prev = i + offset;
                    }
                }

                try updateMap(&map, ptr[prev + 1 .. secondIdx + offset]);
                lastIdx = secondIdx + offset;
            },
        }

        offset += lookahead;
    }

    for (offset..ptr.len) |i| {
        if (ptr[i] == '\n') {
            try updateMap(&map, ptr[lastIdx + 1 .. i]);
            lastIdx = i;
        }
    }
}

fn parallelMMap(allocator: std.mem.Allocator, ptr: []u8) !void {
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
