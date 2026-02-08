const std = @import("std");
const Io = std.Io;

/// Retry policy configuration
/// Timeout configuration for operations
pub const TimeoutConfig = struct {
    /// Timeout for connect operations (milliseconds). null = no timeout
    connect_timeout_ms: ?u32 = 5000,
    /// Timeout for individual read operations (milliseconds). null = no timeout
    read_timeout_ms: ?u32 = 30000,
    /// Timeout for individual write operations (milliseconds). null = no timeout
    write_timeout_ms: ?u32 = 10000,
    /// Overall operation timeout (milliseconds). null = no timeout
    /// This is the maximum time for a complete request-response cycle
    operation_timeout_ms: ?u32 = 60000,

    pub const no_timeout: TimeoutConfig = .{
        .connect_timeout_ms = null,
        .read_timeout_ms = null,
        .write_timeout_ms = null,
        .operation_timeout_ms = null,
    };

    pub const fast: TimeoutConfig = .{
        .connect_timeout_ms = 1000,
        .read_timeout_ms = 5000,
        .write_timeout_ms = 2000,
        .operation_timeout_ms = 10000,
    };

    pub const default: TimeoutConfig = .{
        .connect_timeout_ms = 5000,
        .read_timeout_ms = 30000,
        .write_timeout_ms = 10000,
        .operation_timeout_ms = 60000,
    };

    /// Convert milliseconds to Io.Timeout
    pub fn toIoTimeout(ms: ?u32) Io.Timeout {
        if (ms) |timeout_ms| {
            return .{ .duration = Io.Duration.fromMilliseconds(@intCast(timeout_ms)) };
        }
        return .none;
    }
};
