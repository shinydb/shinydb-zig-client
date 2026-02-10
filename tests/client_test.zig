const std = @import("std");
const testing = std.testing;

const sdb = @import("shinydb_zig_client");
const ShinyDbClient = sdb.ShinyDbClient;
const RetryPolicy = sdb.RetryPolicy;
const CircuitBreaker = sdb.CircuitBreaker;
const TimeoutConfig = sdb.TimeoutConfig;
const ClientError = sdb.client_error.ClientError;

// ============================================================================
// Role Tests
// ============================================================================

test "Role.fromString — admin" {
    try testing.expectEqual(ShinyDbClient.Role.admin, ShinyDbClient.Role.fromString("admin"));
}

test "Role.fromString — read_write" {
    try testing.expectEqual(ShinyDbClient.Role.read_write, ShinyDbClient.Role.fromString("read_write"));
}

test "Role.fromString — read_only" {
    try testing.expectEqual(ShinyDbClient.Role.read_only, ShinyDbClient.Role.fromString("read_only"));
}

test "Role.fromString — unknown returns none" {
    try testing.expectEqual(ShinyDbClient.Role.none, ShinyDbClient.Role.fromString("superuser"));
    try testing.expectEqual(ShinyDbClient.Role.none, ShinyDbClient.Role.fromString(""));
    try testing.expectEqual(ShinyDbClient.Role.none, ShinyDbClient.Role.fromString("ADMIN"));
}

test "Role.toString round-trip" {
    const roles = [_]ShinyDbClient.Role{ .admin, .read_write, .read_only, .none };
    for (roles) |role| {
        const str = role.toString();
        const back = ShinyDbClient.Role.fromString(str);
        try testing.expectEqual(role, back);
    }
}

test "Role.toString values" {
    try testing.expectEqualStrings("admin", ShinyDbClient.Role.admin.toString());
    try testing.expectEqualStrings("read_write", ShinyDbClient.Role.read_write.toString());
    try testing.expectEqualStrings("read_only", ShinyDbClient.Role.read_only.toString());
    try testing.expectEqualStrings("none", ShinyDbClient.Role.none.toString());
}

// ============================================================================
// RetryPolicy Tests
// ============================================================================

test "RetryPolicy default values" {
    const policy = RetryPolicy{};
    try testing.expectEqual(@as(u32, 3), policy.max_attempts);
    try testing.expectEqual(@as(u32, 100), policy.initial_backoff_ms);
    try testing.expectEqual(@as(u32, 10000), policy.max_backoff_ms);
    try testing.expectEqual(@as(f32, 2.0), policy.backoff_multiplier);
}

test "RetryPolicy.calculateBackoff — attempt 0 returns 0" {
    const policy = RetryPolicy{};
    try testing.expectEqual(@as(u32, 0), policy.calculateBackoff(0));
}

test "RetryPolicy.calculateBackoff — attempt 1 returns initial" {
    const policy = RetryPolicy{};
    try testing.expectEqual(@as(u32, 100), policy.calculateBackoff(1));
}

test "RetryPolicy.calculateBackoff — attempt 2 returns doubled" {
    const policy = RetryPolicy{};
    try testing.expectEqual(@as(u32, 200), policy.calculateBackoff(2));
}

test "RetryPolicy.calculateBackoff — attempt 3 returns quadrupled" {
    const policy = RetryPolicy{};
    try testing.expectEqual(@as(u32, 400), policy.calculateBackoff(3));
}

test "RetryPolicy.calculateBackoff — capped at max" {
    const policy = RetryPolicy{ .max_backoff_ms = 500 };
    // attempt 4 would be 100 * 2^3 = 800, but capped at 500
    try testing.expectEqual(@as(u32, 500), policy.calculateBackoff(4));
}

test "RetryPolicy.isRetryable — transient connection errors" {
    try testing.expect(RetryPolicy.isRetryable(error.ConnectionFailed));
    try testing.expect(RetryPolicy.isRetryable(error.ConnectionReset));
    try testing.expect(RetryPolicy.isRetryable(error.ConnectionRefused));
    try testing.expect(RetryPolicy.isRetryable(error.NetworkError));
}

test "RetryPolicy.isRetryable — transient timeout errors" {
    try testing.expect(RetryPolicy.isRetryable(error.Timeout));
    try testing.expect(RetryPolicy.isRetryable(error.ReadTimeout));
    try testing.expect(RetryPolicy.isRetryable(error.WriteTimeout));
}

test "RetryPolicy.isRetryable — transient resource errors" {
    try testing.expect(RetryPolicy.isRetryable(error.PipelineFull));
    try testing.expect(RetryPolicy.isRetryable(error.BufferOverflow));
    try testing.expect(RetryPolicy.isRetryable(error.ServerError));
    try testing.expect(RetryPolicy.isRetryable(error.ServiceUnavailable));
}

test "RetryPolicy.isRetryable — permanent errors return false" {
    try testing.expect(!RetryPolicy.isRetryable(error.InvalidResponse));
    try testing.expect(!RetryPolicy.isRetryable(error.InvalidRequest));
    try testing.expect(!RetryPolicy.isRetryable(error.ProtocolError));
    try testing.expect(!RetryPolicy.isRetryable(error.NotFound));
    try testing.expect(!RetryPolicy.isRetryable(error.PermissionDenied));
}

// ============================================================================
// CircuitBreaker Tests
// ============================================================================

test "CircuitBreaker — init state is closed" {
    var cb = CircuitBreaker.init(5, 2, 30000);
    try testing.expectEqual(CircuitBreaker.State.closed, cb.getState());
}

test "CircuitBreaker — closed allows requests" {
    var cb = CircuitBreaker.init(5, 2, 30000);
    try testing.expect(cb.shouldAllow());
}

test "CircuitBreaker — closed to open after failure threshold" {
    var cb = CircuitBreaker.init(3, 2, 30000);

    // Record 3 failures (threshold = 3)
    cb.recordFailure();
    try testing.expectEqual(CircuitBreaker.State.closed, cb.getState());
    cb.recordFailure();
    try testing.expectEqual(CircuitBreaker.State.closed, cb.getState());
    cb.recordFailure();

    // Should now be open
    try testing.expectEqual(CircuitBreaker.State.open, cb.getState());
}

test "CircuitBreaker — open rejects requests" {
    var cb = CircuitBreaker.init(2, 2, 60000); // 60s timeout so it won't transition

    cb.recordFailure();
    cb.recordFailure(); // threshold reached -> open

    try testing.expectEqual(CircuitBreaker.State.open, cb.getState());
    try testing.expect(!cb.shouldAllow());
}

test "CircuitBreaker — success resets failure count in closed state" {
    var cb = CircuitBreaker.init(3, 2, 30000);

    cb.recordFailure();
    cb.recordFailure();
    // 2 failures, not yet at threshold
    try testing.expectEqual(CircuitBreaker.State.closed, cb.getState());

    cb.recordSuccess();
    // Failure count should be reset

    // Now need 3 more failures to open
    cb.recordFailure();
    cb.recordFailure();
    try testing.expectEqual(CircuitBreaker.State.closed, cb.getState());
}

test "CircuitBreaker — reset returns to closed" {
    var cb = CircuitBreaker.init(2, 2, 30000);

    cb.recordFailure();
    cb.recordFailure(); // -> open
    try testing.expectEqual(CircuitBreaker.State.open, cb.getState());

    cb.reset();
    try testing.expectEqual(CircuitBreaker.State.closed, cb.getState());
    try testing.expect(cb.shouldAllow());
}

test "CircuitBreaker — half-open failure returns to open" {
    var cb = CircuitBreaker.init(2, 2, 0); // 0ms timeout for instant transition

    cb.recordFailure();
    cb.recordFailure(); // -> open

    // With 0ms timeout, shouldAllow transitions to half-open
    try testing.expect(cb.shouldAllow());
    try testing.expectEqual(CircuitBreaker.State.half_open, cb.getState());

    // Failure in half-open goes back to open
    cb.recordFailure();
    try testing.expectEqual(CircuitBreaker.State.open, cb.getState());
}

test "CircuitBreaker — half-open success closes circuit" {
    var cb = CircuitBreaker.init(2, 2, 0); // 0ms timeout

    cb.recordFailure();
    cb.recordFailure(); // -> open

    // Transition to half-open
    try testing.expect(cb.shouldAllow());
    try testing.expectEqual(CircuitBreaker.State.half_open, cb.getState());

    // 2 successes (threshold = 2) should close it
    cb.recordSuccess();
    try testing.expectEqual(CircuitBreaker.State.half_open, cb.getState());
    cb.recordSuccess();
    try testing.expectEqual(CircuitBreaker.State.closed, cb.getState());
}

// ============================================================================
// TimeoutConfig Tests
// ============================================================================

test "TimeoutConfig.default values" {
    const cfg = TimeoutConfig.default;
    try testing.expectEqual(@as(u32, 5000), cfg.connect_timeout_ms.?);
    try testing.expectEqual(@as(u32, 30000), cfg.read_timeout_ms.?);
    try testing.expectEqual(@as(u32, 10000), cfg.write_timeout_ms.?);
    try testing.expectEqual(@as(u32, 60000), cfg.operation_timeout_ms.?);
}

test "TimeoutConfig.fast values" {
    const cfg = TimeoutConfig.fast;
    try testing.expectEqual(@as(u32, 1000), cfg.connect_timeout_ms.?);
    try testing.expectEqual(@as(u32, 5000), cfg.read_timeout_ms.?);
    try testing.expectEqual(@as(u32, 2000), cfg.write_timeout_ms.?);
    try testing.expectEqual(@as(u32, 10000), cfg.operation_timeout_ms.?);
}

test "TimeoutConfig.no_timeout — all null" {
    const cfg = TimeoutConfig.no_timeout;
    try testing.expect(cfg.connect_timeout_ms == null);
    try testing.expect(cfg.read_timeout_ms == null);
    try testing.expect(cfg.write_timeout_ms == null);
    try testing.expect(cfg.operation_timeout_ms == null);
}

test "TimeoutConfig — custom values" {
    const cfg = TimeoutConfig{
        .connect_timeout_ms = 2000,
        .read_timeout_ms = 15000,
        .write_timeout_ms = 5000,
        .operation_timeout_ms = 30000,
    };
    try testing.expectEqual(@as(u32, 2000), cfg.connect_timeout_ms.?);
    try testing.expectEqual(@as(u32, 15000), cfg.read_timeout_ms.?);
    try testing.expectEqual(@as(u32, 5000), cfg.write_timeout_ms.?);
    try testing.expectEqual(@as(u32, 30000), cfg.operation_timeout_ms.?);
}

// ============================================================================
// isTimeoutError Tests
// ============================================================================

test "isTimeoutError — timeout errors return true" {
    try testing.expect(ShinyDbClient.isTimeoutError(error.Timeout));
    try testing.expect(ShinyDbClient.isTimeoutError(error.ReadTimeout));
    try testing.expect(ShinyDbClient.isTimeoutError(error.WriteTimeout));
}

test "isTimeoutError — non-timeout errors return false" {
    try testing.expect(!ShinyDbClient.isTimeoutError(error.ConnectionFailed));
    try testing.expect(!ShinyDbClient.isTimeoutError(error.InvalidResponse));
    try testing.expect(!ShinyDbClient.isTimeoutError(error.ServerError));
}
