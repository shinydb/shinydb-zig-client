const std = @import("std");
const milliTimestamp = @import("common.zig").milliTimestamp;

/// Operation types for metrics tracking
pub const OperationType = enum {
    insert,
    get,
    update,
    delete,
    scan,
    range_query,
    find,
    ping,
    batch_create,
    flush,
    // Security operations
    authenticate,
    create_user,
    delete_user,
    change_password,
    logout,
    // Transaction operations
    begin_txn,
    commit_txn,
    abort_txn,
    txn_put,
    txn_delete,
    // Backup/Restore operations
    backup,
    restore,
    other,

    pub fn toString(self: OperationType) []const u8 {
        return switch (self) {
            .insert => "insert",
            .get => "get",
            .update => "update",
            .delete => "delete",
            .scan => "scan",
            .range_query => "range_query",
            .find => "find",
            .ping => "ping",
            .batch_create => "batch_create",
            .flush => "flush",
            .authenticate => "authenticate",
            .create_user => "create_user",
            .delete_user => "delete_user",
            .change_password => "change_password",
            .logout => "logout",
            .begin_txn => "begin_txn",
            .commit_txn => "commit_txn",
            .abort_txn => "abort_txn",
            .txn_put => "txn_put",
            .txn_delete => "txn_delete",
            .backup => "backup",
            .restore => "restore",
            .other => "other",
        };
    }
};

/// Result of an operation for metrics
pub const OperationResult = enum {
    success,
    failure,
    timeout,
};

/// Statistics for a single operation type
pub const OperationStats = struct {
    total_count: u64 = 0,
    success_count: u64 = 0,
    failure_count: u64 = 0,
    timeout_count: u64 = 0,

    // Latency tracking (microseconds)
    total_latency_us: u64 = 0,
    min_latency_us: u64 = std.math.maxInt(u64),
    max_latency_us: u64 = 0,

    // Histogram buckets (in microseconds)
    // Buckets: <100us, <500us, <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, <1s, >=1s
    histogram: [10]u64 = [_]u64{0} ** 10,

    pub fn record(self: *OperationStats, latency_us: u64, result: OperationResult) void {
        self.total_count += 1;
        self.total_latency_us += latency_us;

        switch (result) {
            .success => self.success_count += 1,
            .failure => self.failure_count += 1,
            .timeout => self.timeout_count += 1,
        }

        if (latency_us < self.min_latency_us) {
            self.min_latency_us = latency_us;
        }
        if (latency_us > self.max_latency_us) {
            self.max_latency_us = latency_us;
        }

        // Update histogram
        const bucket = self.getBucket(latency_us);
        self.histogram[bucket] += 1;
    }

    fn getBucket(self: *const OperationStats, latency_us: u64) usize {
        _ = self;
        if (latency_us < 100) return 0;
        if (latency_us < 500) return 1;
        if (latency_us < 1_000) return 2;
        if (latency_us < 5_000) return 3;
        if (latency_us < 10_000) return 4;
        if (latency_us < 50_000) return 5;
        if (latency_us < 100_000) return 6;
        if (latency_us < 500_000) return 7;
        if (latency_us < 1_000_000) return 8;
        return 9;
    }

    pub fn getAvgLatencyUs(self: *const OperationStats) u64 {
        if (self.total_count == 0) return 0;
        return self.total_latency_us / self.total_count;
    }

    pub fn getSuccessRate(self: *const OperationStats) f64 {
        if (self.total_count == 0) return 0.0;
        return @as(f64, @floatFromInt(self.success_count)) / @as(f64, @floatFromInt(self.total_count));
    }

    /// Get approximate percentile from histogram
    pub fn getPercentile(self: *const OperationStats, percentile: f64) u64 {
        if (self.total_count == 0) return 0;

        const target = @as(u64, @intFromFloat(@as(f64, @floatFromInt(self.total_count)) * percentile / 100.0));
        var cumulative: u64 = 0;

        const bucket_max = [_]u64{ 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 10_000_000 };

        for (self.histogram, 0..) |count, i| {
            cumulative += count;
            if (cumulative >= target) {
                return bucket_max[i];
            }
        }

        return bucket_max[9];
    }

    pub fn reset(self: *OperationStats) void {
        self.* = .{};
    }
};

/// Callback for operation metrics
pub const MetricsCallback = *const fn (op_type: OperationType, latency_us: u64, result: OperationResult) void;

/// Client metrics tracker
pub const ClientMetrics = struct {
    allocator: std.mem.Allocator,

    // Per-operation stats
    stats: [@typeInfo(OperationType).@"enum".fields.len]OperationStats,

    // Global stats
    total_requests: u64,
    total_bytes_sent: u64,
    total_bytes_received: u64,
    connection_count: u32,
    reconnection_count: u32,

    // Timing
    start_time: i64,

    // Optional callback
    callback: ?MetricsCallback,

    // Lock for thread safety
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) ClientMetrics {
        return .{
            .allocator = allocator,
            .stats = [_]OperationStats{.{}} ** @typeInfo(OperationType).@"enum".fields.len,
            .total_requests = 0,
            .total_bytes_sent = 0,
            .total_bytes_received = 0,
            .connection_count = 0,
            .reconnection_count = 0,
            .start_time = milliTimestamp(),
            .callback = null,
            .mutex = .{},
        };
    }

    pub fn deinit(self: *ClientMetrics) void {
        _ = self;
        // Nothing to clean up for now
    }

    /// Set callback for operation metrics
    pub fn setCallback(self: *ClientMetrics, callback: ?MetricsCallback) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.callback = callback;
    }

    /// Record an operation
    pub fn recordOperation(self: *ClientMetrics, op_type: OperationType, latency_us: u64, result: OperationResult) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.total_requests += 1;
        self.stats[@intFromEnum(op_type)].record(latency_us, result);

        // Call callback if set
        if (self.callback) |cb| {
            cb(op_type, latency_us, result);
        }
    }

    /// Record bytes sent
    pub fn recordBytesSent(self: *ClientMetrics, bytes: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.total_bytes_sent += bytes;
    }

    /// Record bytes received
    pub fn recordBytesReceived(self: *ClientMetrics, bytes: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.total_bytes_received += bytes;
    }

    /// Record connection event
    pub fn recordConnection(self: *ClientMetrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.connection_count += 1;
    }

    /// Record reconnection event
    pub fn recordReconnection(self: *ClientMetrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.reconnection_count += 1;
    }

    /// Get stats for a specific operation type
    pub fn getStats(self: *ClientMetrics, op_type: OperationType) OperationStats {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.stats[@intFromEnum(op_type)];
    }

    /// Get total throughput (ops/sec)
    pub fn getThroughput(self: *ClientMetrics) f64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const elapsed_ms = milliTimestamp() - self.start_time;
        if (elapsed_ms <= 0) return 0.0;

        return @as(f64, @floatFromInt(self.total_requests)) * 1000.0 / @as(f64, @floatFromInt(elapsed_ms));
    }

    /// Get uptime in milliseconds
    pub fn getUptimeMs(self: *ClientMetrics) i64 {
        return milliTimestamp() - self.start_time;
    }

    /// Reset all metrics
    pub fn reset(self: *ClientMetrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (&self.stats) |*stat| {
            stat.reset();
        }
        self.total_requests = 0;
        self.total_bytes_sent = 0;
        self.total_bytes_received = 0;
        self.start_time = milliTimestamp();
        // Don't reset connection counts
    }

    /// Get a snapshot of all metrics (for reporting)
    pub const MetricsSnapshot = struct {
        uptime_ms: i64,
        total_requests: u64,
        throughput: f64,
        total_bytes_sent: u64,
        total_bytes_received: u64,
        connection_count: u32,
        reconnection_count: u32,
        stats: [@typeInfo(OperationType).@"enum".fields.len]OperationStats,
    };

    pub fn getSnapshot(self: *ClientMetrics) MetricsSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        const elapsed_ms = milliTimestamp() - self.start_time;
        const throughput = if (elapsed_ms > 0)
            @as(f64, @floatFromInt(self.total_requests)) * 1000.0 / @as(f64, @floatFromInt(elapsed_ms))
        else
            0.0;

        return .{
            .uptime_ms = elapsed_ms,
            .total_requests = self.total_requests,
            .throughput = throughput,
            .total_bytes_sent = self.total_bytes_sent,
            .total_bytes_received = self.total_bytes_received,
            .connection_count = self.connection_count,
            .reconnection_count = self.reconnection_count,
            .stats = self.stats,
        };
    }

    /// Print metrics summary to debug output
    pub fn printSummary(self: *ClientMetrics) void {
        const snapshot = self.getSnapshot();

        std.debug.print("\n=== Client Metrics Summary ===\n", .{});
        std.debug.print("Uptime: {d}ms\n", .{snapshot.uptime_ms});
        std.debug.print("Total Requests: {d}\n", .{snapshot.total_requests});
        std.debug.print("Throughput: {d:.2} ops/sec\n", .{snapshot.throughput});
        std.debug.print("Bytes Sent: {d}\n", .{snapshot.total_bytes_sent});
        std.debug.print("Bytes Received: {d}\n", .{snapshot.total_bytes_received});
        std.debug.print("Connections: {d}\n", .{snapshot.connection_count});
        std.debug.print("Reconnections: {d}\n\n", .{snapshot.reconnection_count});

        std.debug.print("Operation Statistics:\n", .{});
        std.debug.print("{s:<15} {s:>10} {s:>10} {s:>10} {s:>10} {s:>12} {s:>12} {s:>12}\n", .{ "Operation", "Total", "Success", "Failure", "Timeout", "Avg(us)", "P50(us)", "P99(us)" });
        std.debug.print("{s:-<95}\n", .{""});

        inline for (@typeInfo(OperationType).@"enum".fields, 0..) |field, i| {
            const stat = &snapshot.stats[i];
            if (stat.total_count > 0) {
                std.debug.print("{s:<15} {d:>10} {d:>10} {d:>10} {d:>10} {d:>12} {d:>12} {d:>12}\n", .{
                    field.name,
                    stat.total_count,
                    stat.success_count,
                    stat.failure_count,
                    stat.timeout_count,
                    stat.getAvgLatencyUs(),
                    stat.getPercentile(50),
                    stat.getPercentile(99),
                });
            }
        }

        std.debug.print("\n", .{});
    }
};

/// Timer for measuring operation latency
pub const Timer = struct {
    started: std.time.Instant,

    pub fn start() Timer {
        return .{
            .started = std.time.Instant.now() catch unreachable,
        };
    }

    /// Returns elapsed time in microseconds
    pub fn elapsedUs(self: *const Timer) u64 {
        const now = std.time.Instant.now() catch return 0;
        const elapsed_ns = now.since(self.started);
        return elapsed_ns / 1000;
    }

    /// Returns elapsed time in milliseconds
    pub fn elapsedMs(self: *const Timer) u64 {
        const now = std.time.Instant.now() catch return 0;
        const elapsed_ns = now.since(self.started);
        return elapsed_ns / 1_000_000;
    }
};

/// Log levels for debug logging
pub const LogLevel = enum(u8) {
    trace = 0,
    debug = 1,
    info = 2,
    warn = 3,
    err = 4,
    off = 5,

    pub fn toString(self: LogLevel) []const u8 {
        return switch (self) {
            .trace => "TRACE",
            .debug => "DEBUG",
            .info => "INFO",
            .warn => "WARN",
            .err => "ERROR",
            .off => "OFF",
        };
    }
};

/// Simple logger with configurable level
pub const Logger = struct {
    level: LogLevel = .info,
    prefix: []const u8 = "[shinydb]",

    pub fn init(level: LogLevel) Logger {
        return .{ .level = level };
    }

    pub fn setLevel(self: *Logger, level: LogLevel) void {
        self.level = level;
    }

    fn shouldLog(self: *const Logger, level: LogLevel) bool {
        return @intFromEnum(level) >= @intFromEnum(self.level);
    }

    pub fn trace(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        if (self.shouldLog(.trace)) {
            std.debug.print("{s} [TRACE] " ++ fmt ++ "\n", .{self.prefix} ++ args);
        }
    }

    pub fn debug(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        if (self.shouldLog(.debug)) {
            std.debug.print("{s} [DEBUG] " ++ fmt ++ "\n", .{self.prefix} ++ args);
        }
    }

    pub fn info(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        if (self.shouldLog(.info)) {
            std.debug.print("{s} [INFO] " ++ fmt ++ "\n", .{self.prefix} ++ args);
        }
    }

    pub fn warn(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        if (self.shouldLog(.warn)) {
            std.debug.print("{s} [WARN] " ++ fmt ++ "\n", .{self.prefix} ++ args);
        }
    }

    pub fn err(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        if (self.shouldLog(.err)) {
            std.debug.print("{s} [ERROR] " ++ fmt ++ "\n", .{self.prefix} ++ args);
        }
    }
};

// Global logger instance (can be configured by user)
pub var logger: Logger = .{ .level = .warn };

/// Set global log level
pub fn setLogLevel(level: LogLevel) void {
    logger.setLevel(level);
}
