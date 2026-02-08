const std = @import("std");
const ClientError = @import("client_error.zig").ClientError;

pub const RetryPolicy = struct {
    max_attempts: u32 = 3,
    initial_backoff_ms: u32 = 100,
    max_backoff_ms: u32 = 10000,
    backoff_multiplier: f32 = 2.0,

    /// Check if an error is retryable
    pub fn isRetryable(err: ClientError) bool {
        return switch (err) {
            // Transient errors - can retry
            error.ConnectionFailed,
            error.ConnectionReset,
            error.ConnectionRefused,
            error.NetworkError,
            error.Timeout,
            error.ReadTimeout,
            error.WriteTimeout,
            error.PipelineFull,
            error.BufferOverflow,
            error.ServerError,
            error.ServiceUnavailable,
            => true,

            // Permanent errors - don't retry
            error.InvalidResponse,
            error.InvalidRequest,
            error.ProtocolError,
            error.NotFound,
            error.PermissionDenied,
            => false,
        };
    }

    /// Calculate backoff delay for attempt number
    pub fn calculateBackoff(self: *const RetryPolicy, attempt: u32) u32 {
        if (attempt == 0) return 0;

        const delay_f = @as(f32, @floatFromInt(self.initial_backoff_ms)) *
            std.math.pow(f32, self.backoff_multiplier, @as(f32, @floatFromInt(attempt - 1)));
        const delay = @as(u32, @intFromFloat(@min(delay_f, @as(f32, @floatFromInt(self.max_backoff_ms)))));
        return delay;
    }
};
