const std = @import("std");
const builtin = @import("builtin");
const native_os = builtin.os.tag;

/// Get current time in milliseconds since Unix epoch (wall clock).
/// Cross-platform: works on POSIX, Windows, and WASI.
pub fn milliTimestamp() i64 {
    switch (native_os) {
        .windows => {
            // RtlGetSystemTimePrecise returns 100ns intervals since Windows epoch (1601-01-01)
            const ft = std.os.windows.ntdll.RtlGetSystemTimePrecise();
            // Convert to Unix epoch: subtract difference in 100ns units, then to ms
            const unix_100ns = ft - 116444736000000000;
            return @intCast(@divTrunc(unix_100ns, 10_000));
        },
        .wasi => {
            var ns: std.os.wasi.timestamp_t = undefined;
            return switch (std.os.wasi.clock_time_get(.REALTIME, 1_000_000, &ns)) {
                .SUCCESS => @as(i64, @intCast(@divTrunc(ns, 1_000_000))),
                else => 0,
            };
        },
        else => {
            // POSIX systems (Linux, macOS, BSD, etc.)
            var ts: std.c.timespec = undefined;
            _ = std.c.clock_gettime(std.c.CLOCK.REALTIME, &ts);
            const seconds: i64 = @intCast(ts.sec);
            const nanos: i64 = @intCast(ts.nsec);
            return seconds * 1000 + @divTrunc(nanos, 1_000_000);
        },
    }
}

/// Get current monotonic time in nanoseconds for measuring elapsed time.
/// Cross-platform: works on POSIX, Windows, and WASI.
pub fn nanoTimestamp() i128 {
    switch (native_os) {
        .windows => {
            const windows = std.os.windows;
            const qpf: u64 = qpf: {
                var f: windows.LARGE_INTEGER = undefined;
                std.debug.assert(windows.ntdll.RtlQueryPerformanceFrequency(&f) != windows.FALSE);
                break :qpf @bitCast(f);
            };
            const qpc: u64 = qpc: {
                var c: windows.LARGE_INTEGER = undefined;
                std.debug.assert(windows.ntdll.RtlQueryPerformanceCounter(&c) != windows.FALSE);
                break :qpc @bitCast(c);
            };
            // 10MHz (1 tick per 100ns) is the common QPF on Windows
            const common_qpf = 10_000_000;
            if (qpf == common_qpf) return @as(i128, qpc) * (std.time.ns_per_s / common_qpf);
            return @as(i128, qpc) * std.time.ns_per_s / @as(i128, qpf);
        },
        .wasi => {
            var ns: std.os.wasi.timestamp_t = undefined;
            return switch (std.os.wasi.clock_time_get(.MONOTONIC, 1, &ns)) {
                .SUCCESS => @intCast(ns),
                else => 0,
            };
        },
        else => {
            // POSIX systems (Linux, macOS, BSD, etc.)
            var ts: std.c.timespec = undefined;
            _ = std.c.clock_gettime(std.c.CLOCK.MONOTONIC, &ts);
            const seconds: i128 = @intCast(ts.sec);
            const nanos: i128 = @intCast(ts.nsec);
            return seconds * std.time.ns_per_s + nanos;
        },
    }
}
