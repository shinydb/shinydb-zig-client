const std = @import("std");
const posix = std.posix;

/// Get current time in milliseconds since Unix epoch
pub fn milliTimestamp() i64 {
    const ts = posix.clock_gettime(posix.CLOCK.REALTIME) catch return 0;
    const seconds: i64 = @intCast(ts.sec);
    const nanos: i64 = @intCast(ts.nsec);
    return seconds * 1000 + @divTrunc(nanos, 1_000_000);
}
