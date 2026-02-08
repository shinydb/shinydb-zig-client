const std = @import("std");
const milliTimestamp = @import("common.zig").milliTimestamp;


/// Circuit breaker for preventing cascading failures
pub const CircuitBreaker = struct {
    state: State,
    failure_count: u32,
    success_count: u32,
    last_state_change: i64,

    // Configuration
    failure_threshold: u32, // Consecutive failures before opening circuit
    success_threshold: u32, // Successes in half-open before closing
    timeout_ms: u32, // Time to wait before trying half-open

    const State = enum {
        closed, // Normal operation
        open, // Too many failures, reject requests immediately
        half_open, // Testing if service recovered
    };

    pub fn init(failure_threshold: u32, success_threshold: u32, timeout_ms: u32) CircuitBreaker {
        return .{
            .state = .closed,
            .failure_count = 0,
            .success_count = 0,
            .last_state_change = milliTimestamp(),
            .failure_threshold = failure_threshold,
            .success_threshold = success_threshold,
            .timeout_ms = timeout_ms,
        };
    }

    /// Check if a request should be allowed through
    pub fn shouldAllow(self: *CircuitBreaker) bool {
        switch (self.state) {
            .closed => return true,
            .half_open => return true,
            .open => {
                const now = milliTimestamp();
                const elapsed = @as(u32, @intCast(now - self.last_state_change));
                if (elapsed >= self.timeout_ms) {
                    // Transition to half-open to test recovery
                    self.state = .half_open;
                    self.failure_count = 0;
                    self.success_count = 0;
                    self.last_state_change = now;
                    return true;
                }
                return false;
            },
        }
    }

    /// Record a successful operation
    pub fn recordSuccess(self: *CircuitBreaker) void {
        switch (self.state) {
            .closed => {
                // Reset failure count on success in closed state
                self.failure_count = 0;
            },
            .half_open => {
                self.success_count += 1;
                if (self.success_count >= self.success_threshold) {
                    // Recovered! Close the circuit
                    self.state = .closed;
                    self.failure_count = 0;
                    self.success_count = 0;
                    self.last_state_change = milliTimestamp();
                }
            },
            .open => {
                // Shouldn't happen, but just in case
                self.failure_count = 0;
            },
        }
    }

    /// Record a failed operation
    pub fn recordFailure(self: *CircuitBreaker) void {
        self.failure_count += 1;

        switch (self.state) {
            .closed => {
                if (self.failure_count >= self.failure_threshold) {
                    // Open the circuit
                    self.state = .open;
                    self.last_state_change = milliTimestamp();
                }
            },
            .half_open => {
                // Failed during test, go back to open
                self.state = .open;
                self.success_count = 0;
                self.last_state_change = milliTimestamp();
            },
            .open => {
                // Already open, update timestamp
                self.last_state_change = milliTimestamp();
            },
        }
    }

    /// Get current circuit breaker state
    pub fn getState(self: *const CircuitBreaker) State {
        return self.state;
    }

    /// Reset the circuit breaker to closed state
    pub fn reset(self: *CircuitBreaker) void {
        self.state = .closed;
        self.failure_count = 0;
        self.success_count = 0;
        self.last_state_change = milliTimestamp();
    }
};
