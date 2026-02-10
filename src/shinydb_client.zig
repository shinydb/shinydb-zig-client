const std = @import("std");
const Io = std.Io;
const net = std.Io.net;
const proto = @import("proto");
const Packet = proto.Packet;
const Operation = proto.Operation;

const ClientError = @import("client_error.zig").ClientError;
const RetryPolicy = @import("retry_policy.zig").RetryPolicy;
const CircuitBreaker = @import("circuit_breaker.zig").CircuitBreaker;
const TimeoutConfig = @import("timeout_config.zig").TimeoutConfig;

const milliTimestamp = @import("common.zig").milliTimestamp;
const metrics_mod = @import("metrics.zig");
pub const ClientMetrics = metrics_mod.ClientMetrics;
pub const OperationType = metrics_mod.OperationType;
pub const OperationResult = metrics_mod.OperationResult;
pub const Timer = metrics_mod.Timer;
pub const Logger = metrics_mod.Logger;
pub const LogLevel = metrics_mod.LogLevel;

/// ShinyDbClient is the main entry point for connecting to shinydb.
/// It provides connection management, space-level operations, and is the base for SpaceClient/StoreClient.
pub const ShinyDbClient = struct {
    allocator: std.mem.Allocator,
    io: Io,
    socket: ?net.Stream,
    session_id: u32,
    packet_id: u32,

    // Connection info (for reconnection)
    host: ?[]const u8,
    port: u16,

    // Retry policy
    retry_policy: RetryPolicy,

    // Circuit breaker
    circuit_breaker: CircuitBreaker,

    // Timeout configuration
    timeout_config: TimeoutConfig,

    // Metrics tracking (optional)
    metrics: ?*ClientMetrics = null,

    // Logger (optional, uses global logger by default)
    logger: ?*Logger = null,

    // Pipelining state
    pending_requests: std.ArrayList(PendingRequest),
    next_correlation_id: u64,

    // Reusable resources
    buffer_writer: proto.BufferWriter,
    recv_buffer: std.ArrayList(u8),

    // Batched flush optimization
    flush_threshold: u32,
    sends_since_flush: u32,

    const PendingRequest = struct {
        correlation_id: u64,
        packet_id: u32,
        timestamp: i64,
    };

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, io: Io) !*Self {
        return initWithFlushThreshold(allocator, io, 10);
    }

    pub fn initWithFlushThreshold(allocator: std.mem.Allocator, io: Io, flush_threshold: u32) !*Self {
        const client = try allocator.create(Self);
        var recv_buffer: std.ArrayList(u8) = .empty;
        try recv_buffer.ensureTotalCapacity(allocator, 64 * 1024);

        client.* = .{
            .allocator = allocator,
            .io = io,
            .socket = null,
            .session_id = 0,
            .packet_id = 0,
            .host = null,
            .port = 0,
            .retry_policy = .{},
            .circuit_breaker = CircuitBreaker.init(5, 2, 30000),
            .timeout_config = TimeoutConfig.default,
            .pending_requests = .empty,
            .next_correlation_id = 1,
            .buffer_writer = try proto.BufferWriter.init(allocator),
            .recv_buffer = recv_buffer,
            .flush_threshold = flush_threshold,
            .sends_since_flush = 0,
        };
        return client;
    }

    pub fn deinit(self: *Self) void {
        self.disconnect();
        if (self.host) |h| {
            self.allocator.free(h);
        }
        self.pending_requests.deinit(self.allocator);
        self.buffer_writer.deinit(self.allocator);
        self.recv_buffer.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    // ============================================================================
    // Connection Management
    // ============================================================================

    pub fn connect(self: *Self, host: []const u8, port: u16) !void {
        self.logDebug("Connecting to {s}:{d}", .{ host, port });

        const address = net.IpAddress.parseIp4(host, port) catch |err| {
            self.logError("Failed to parse address {s}:{d}: {}", .{ host, port, err });
            return err;
        };
        const socket = address.connect(self.io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch |err| {
            self.logError("Connection failed to {s}:{d}: {}", .{ host, port, err });
            return err;
        };
        self.socket = socket;

        if (self.host) |old_host| {
            self.allocator.free(old_host);
        }
        self.host = try self.allocator.dupe(u8, host);
        self.port = port;

        var prng = std.Random.DefaultPrng.init(@intCast(milliTimestamp()));
        self.session_id = prng.random().int(u32);

        self.logInfo("Connected to {s}:{d} (session_id: {x})", .{ host, port, self.session_id });

        if (self.metrics) |m| {
            m.recordConnection();
        }
    }

    pub fn disconnect(self: *Self) void {
        if (self.socket) |*sock| {
            self.logDebug("Disconnecting from {s}:{d}", .{ self.host orelse "unknown", self.port });
            sock.close(self.io);
            self.socket = null;
        }
    }

    pub fn isConnected(self: *Self) bool {
        return self.socket != null;
    }

    pub fn reconnect(self: *Self) !void {
        const old_host = self.host orelse {
            self.logError("Cannot reconnect: no previous host stored", .{});
            return ClientError.ConnectionFailed;
        };
        const port = self.port;

        self.logInfo("Reconnecting to {s}:{d}", .{ old_host, port });

        const host = try self.allocator.dupe(u8, old_host);
        defer self.allocator.free(host);

        self.disconnect();

        const pending_count = self.pending_requests.items.len;
        if (pending_count > 0) {
            self.logWarn("Clearing {d} pending requests due to reconnection", .{pending_count});
        }
        self.pending_requests.clearRetainingCapacity();
        self.next_correlation_id = 1;
        self.packet_id = 0;

        try self.connect(host, port);

        if (self.metrics) |m| {
            m.recordReconnection();
        }
    }

    // ============================================================================
    // Configuration
    // ============================================================================

    pub fn setMetrics(self: *Self, m: ?*ClientMetrics) void {
        self.metrics = m;
    }

    pub fn getMetrics(self: *Self) ?*ClientMetrics {
        return self.metrics;
    }

    pub fn setLogger(self: *Self, log: ?*Logger) void {
        self.logger = log;
    }

    pub fn getLogger(self: *Self) *Logger {
        return self.logger orelse &metrics_mod.logger;
    }

    pub fn setRetryPolicy(self: *Self, policy: RetryPolicy) void {
        self.retry_policy = policy;
    }

    pub fn setTimeoutConfig(self: *Self, config: TimeoutConfig) void {
        self.timeout_config = config;
    }

    pub fn getTimeoutConfig(self: *const Self) TimeoutConfig {
        return self.timeout_config;
    }

    pub fn setCircuitBreaker(self: *Self, breaker: CircuitBreaker) void {
        self.circuit_breaker = breaker;
    }

    pub fn getCircuitBreakerState(self: *const Self) CircuitBreaker.State {
        return self.circuit_breaker.getState();
    }

    pub fn resetCircuitBreaker(self: *Self) void {
        self.circuit_breaker.reset();
    }

    // ============================================================================
    // Logging
    // ============================================================================

    fn logDebug(self: *Self, comptime fmt: []const u8, args: anytype) void {
        self.getLogger().debug(fmt, args);
    }

    fn logInfo(self: *Self, comptime fmt: []const u8, args: anytype) void {
        self.getLogger().info(fmt, args);
    }

    fn logWarn(self: *Self, comptime fmt: []const u8, args: anytype) void {
        self.getLogger().warn(fmt, args);
    }

    fn logError(self: *Self, comptime fmt: []const u8, args: anytype) void {
        self.getLogger().err(fmt, args);
    }

    // ============================================================================
    // Low-level Send/Receive
    // ============================================================================

    pub fn sendAsync(self: *Self, op: Operation) !u64 {
        return self.sendAsyncWithTimeout(op, self.timeout_config.write_timeout_ms);
    }

    pub fn sendAsyncWithTimeout(self: *Self, op: Operation, timeout_ms: ?u32) !u64 {
        if (self.socket == null) {
            return ClientError.ConnectionFailed;
        }

        const start_time = milliTimestamp();
        const deadline: ?i64 = if (timeout_ms) |t| start_time + @as(i64, t) else null;

        const correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        const packet = Packet{
            .checksum = 0,
            .packet_length = 0,
            .packet_id = self.packet_id,
            .session_id = self.session_id,
            .correlation_id = correlation_id,
            .timestamp = milliTimestamp(),
            .op = op,
        };
        self.packet_id += 1;

        self.buffer_writer.reset();
        const serialized = try packet.serialize(&self.buffer_writer);

        var write_buffer: [64 * 1024]u8 = undefined;
        var writer = self.socket.?.writer(self.io, &write_buffer);

        var length_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &length_buf, @intCast(serialized.len), .little);

        writer.interface.writeAll(&length_buf) catch |err| {
            if (deadline) |d| {
                if (milliTimestamp() > d) return ClientError.WriteTimeout;
            }
            if (err == error.WriteFailed) return ClientError.ConnectionReset;
            return err;
        };

        writer.interface.writeAll(serialized) catch |err| {
            if (deadline) |d| {
                if (milliTimestamp() > d) return ClientError.WriteTimeout;
            }
            if (err == error.WriteFailed) return ClientError.ConnectionReset;
            return err;
        };

        writer.interface.flush() catch |err| {
            if (deadline) |d| {
                if (milliTimestamp() > d) return ClientError.WriteTimeout;
            }
            if (err == error.WriteFailed) return ClientError.ConnectionReset;
            return err;
        };

        if (deadline) |d| {
            if (milliTimestamp() > d) return ClientError.WriteTimeout;
        }

        try self.pending_requests.append(self.allocator, .{
            .correlation_id = correlation_id,
            .packet_id = packet.packet_id,
            .timestamp = packet.timestamp,
        });

        return correlation_id;
    }

    pub fn receiveAsync(self: *Self) !Packet {
        return self.receiveAsyncWithTimeout(self.timeout_config.read_timeout_ms);
    }

    pub fn receiveAsyncWithTimeout(self: *Self, timeout_ms: ?u32) !Packet {
        if (self.socket == null) {
            return ClientError.ConnectionFailed;
        }

        if (self.pending_requests.items.len == 0) {
            return ClientError.InvalidResponse;
        }

        const start_time = milliTimestamp();
        const deadline: ?i64 = if (timeout_ms) |t| start_time + @as(i64, t) else null;

        var read_buffer: [64 * 1024]u8 = undefined;
        var reader = self.socket.?.reader(self.io, &read_buffer);

        var resp_length_buf: [4]u8 = undefined;
        reader.interface.readSliceAll(&resp_length_buf) catch |err| {
            if (deadline) |d| {
                if (milliTimestamp() > d) return ClientError.ReadTimeout;
            }
            if (err == error.EndOfStream or err == error.ReadFailed) {
                return ClientError.ConnectionReset;
            }
            return err;
        };

        if (deadline) |d| {
            if (milliTimestamp() > d) return ClientError.ReadTimeout;
        }

        const msg_len = std.mem.readInt(u32, &resp_length_buf, .little);

        if (msg_len > 16 * 1024 * 1024) {
            return ClientError.InvalidResponse;
        }

        if (self.recv_buffer.items.len < msg_len) {
            try self.recv_buffer.resize(self.allocator, msg_len);
        }

        const payload = self.recv_buffer.items[0..msg_len];
        reader.interface.readSliceAll(payload) catch |err| {
            if (deadline) |d| {
                if (milliTimestamp() > d) return ClientError.ReadTimeout;
            }
            if (err == error.EndOfStream or err == error.ReadFailed) {
                return ClientError.ConnectionReset;
            }
            return err;
        };

        if (deadline) |d| {
            if (milliTimestamp() > d) return ClientError.ReadTimeout;
        }

        const packet = try Packet.deserialize(self.allocator, payload);
        _ = self.pending_requests.orderedRemove(0);

        return packet;
    }

    pub fn doOperation(self: *Self, op: Operation) !Packet {
        return self.doOperationWithTimeout(op, self.timeout_config.operation_timeout_ms);
    }

    pub fn doOperationWithTimeout(self: *Self, op: Operation, timeout_ms: ?u32) !Packet {
        const start_time = milliTimestamp();
        const deadline: ?i64 = if (timeout_ms) |t| start_time + @as(i64, t) else null;

        const send_timeout = if (deadline) |d| blk: {
            const remaining = d - milliTimestamp();
            if (remaining <= 0) return ClientError.Timeout;
            break :blk @as(u32, @intCast(remaining));
        } else null;

        _ = try self.sendAsyncWithTimeout(op, send_timeout);

        const recv_timeout = if (deadline) |d| blk: {
            const remaining = d - milliTimestamp();
            if (remaining <= 0) return ClientError.Timeout;
            break :blk @as(u32, @intCast(remaining));
        } else null;

        return self.receiveAsyncWithTimeout(recv_timeout);
    }

    pub fn flushWrites(self: *Self) !void {
        if (self.socket == null) {
            return ClientError.ConnectionFailed;
        }

        if (self.sends_since_flush > 0) {
            var write_buffer: [64 * 1024]u8 = undefined;
            var writer = self.socket.?.writer(self.io, &write_buffer);
            try writer.interface.flush();
            self.sends_since_flush = 0;
        }
    }

    pub fn flushPending(self: *Self, responses: *std.ArrayList(Packet)) !void {
        while (self.pending_requests.items.len > 0) {
            const packet = try self.receiveAsync();
            try responses.append(self.allocator, packet);
        }
    }

    // ============================================================================
    // Timeout Handling
    // ============================================================================

    pub fn handleTimeoutCleanup(self: *Self, reconnect_on_timeout: bool) !void {
        self.pending_requests.clearRetainingCapacity();
        self.circuit_breaker.recordFailure();

        if (reconnect_on_timeout) {
            self.disconnect();
            try self.reconnect();
        }
    }

    pub fn isTimeoutError(err: ClientError) bool {
        return switch (err) {
            error.Timeout, error.ReadTimeout, error.WriteTimeout => true,
            else => false,
        };
    }

    // ============================================================================
    // Retry Support
    // ============================================================================

    pub fn withRetry(
        self: *Self,
        comptime T: type,
        operation_fn: *const fn (*Self) anyerror!T,
    ) !T {
        if (!self.circuit_breaker.shouldAllow()) {
            return ClientError.ServiceUnavailable;
        }

        var attempt: u32 = 0;
        var last_err: ?anyerror = null;

        while (attempt < self.retry_policy.max_attempts) : (attempt += 1) {
            const result = operation_fn(self) catch |err| {
                last_err = err;
                self.circuit_breaker.recordFailure();

                const async_err = @as(ClientError, @errorCast(err)) catch {
                    return err;
                };

                const is_retryable = RetryPolicy.isRetryable(async_err);

                if (!is_retryable) {
                    return err;
                }

                if (attempt + 1 < self.retry_policy.max_attempts) {
                    const delay_ms = self.retry_policy.calculateBackoff(attempt + 1);
                    if (delay_ms > 0) {
                        self.io.sleep(Io.Duration.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};
                    }

                    if (async_err == error.ConnectionFailed or
                        async_err == error.ConnectionReset or
                        async_err == error.ConnectionRefused or
                        async_err == error.NetworkError)
                    {
                        self.reconnect() catch |reconn_err| {
                            last_err = reconn_err;
                            continue;
                        };
                    }
                }

                continue;
            };

            self.circuit_breaker.recordSuccess();
            return result;
        }

        return last_err orelse error.Timeout;
    }

    // ============================================================================
    // Health Check
    // ============================================================================

    pub fn ping(self: *Self) !void {
        const timer = Timer.start();

        if (!self.isConnected()) {
            if (self.metrics) |m| {
                m.recordOperation(.ping, timer.elapsedUs(), .failure);
            }
            return ClientError.ConnectionFailed;
        }

        errdefer {
            if (self.metrics) |m| {
                m.recordOperation(.ping, timer.elapsedUs(), .failure);
            }
        }

        const packet = try self.doOperation(.Flush);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    if (self.metrics) |m| {
                        m.recordOperation(.ping, timer.elapsedUs(), .failure);
                    }
                    return ClientError.InvalidResponse;
                }
            },
            else => {
                if (self.metrics) |m| {
                    m.recordOperation(.ping, timer.elapsedUs(), .failure);
                }
                return ClientError.InvalidResponse;
            },
        }

        if (self.metrics) |m| {
            m.recordOperation(.ping, timer.elapsedUs(), .success);
        }
    }

    /// Flush memtable to disk to ensure data durability
    /// Call this after bulk inserts to persist data immediately
    pub fn flush(self: *Self) !void {
        const timer = Timer.start();

        if (!self.isConnected()) {
            if (self.metrics) |m| {
                m.recordOperation(.flush, timer.elapsedUs(), .failure);
            }
            return ClientError.ConnectionFailed;
        }

        errdefer {
            if (self.metrics) |m| {
                m.recordOperation(.flush, timer.elapsedUs(), .failure);
            }
        }

        const packet = try self.doOperation(.Flush);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    std.debug.print("Flush failed with status: {}\n", .{reply.status});
                    if (reply.data) |data| {
                        std.debug.print("Flush error data: {s}\n", .{data});
                    }
                    if (self.metrics) |m| {
                        m.recordOperation(.flush, timer.elapsedUs(), .failure);
                    }
                    return ClientError.ServerError;
                }
            },
            else => |other| {
                std.debug.print("Flush returned unexpected operation: {}\n", .{other});
                if (self.metrics) |m| {
                    m.recordOperation(.flush, timer.elapsedUs(), .failure);
                }
                return ClientError.InvalidResponse;
            },
        }

        if (self.metrics) |m| {
            m.recordOperation(.flush, timer.elapsedUs(), .success);
        }
    }

    // ============================================================================
    // Backup/Restore Operations
    // ============================================================================

    /// Backup metadata returned from backup operations
    pub const BackupMetadata = struct {
        backup_path: []const u8,
        timestamp: i64,
        size_bytes: u64,
        vlog_count: u16,
        entry_count: u64,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *BackupMetadata) void {
            self.allocator.free(self.backup_path);
        }
    };

    // ============================================================================
    // User Management Operations
    // ============================================================================

    /// User roles for access control
    pub const Role = enum(u8) {
        admin = 0,
        read_write = 1,
        read_only = 2,
        none = 3,

        pub fn fromString(str: []const u8) Role {
            if (std.mem.eql(u8, str, "admin")) return .admin;
            if (std.mem.eql(u8, str, "read_write")) return .read_write;
            if (std.mem.eql(u8, str, "read_only")) return .read_only;
            return .none;
        }

        pub fn toString(self: Role) []const u8 {
            return switch (self) {
                .admin => "admin",
                .read_write => "read_write",
                .read_only => "read_only",
                .none => "none",
            };
        }
    };

    /// Authentication result returned by authenticate/authenticateApiKey
    pub const AuthResult = struct {
        session_id: []const u8,
        api_key: []const u8,
        username: []const u8,
        role: Role,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *AuthResult) void {
            if (self.session_id.len > 0) self.allocator.free(self.session_id);
            if (self.api_key.len > 0) self.allocator.free(self.api_key);
            if (self.username.len > 0) self.allocator.free(self.username);
        }
    };

    /// Authenticate with username and password
    /// Returns AuthResult with session_id, api_key, and role
    /// Caller owns returned AuthResult and must call deinit() to free memory
    pub fn authenticate(self: *Self, username: []const u8, password: []const u8) !AuthResult {
        const timer = Timer.start();
        errdefer {
            if (self.metrics) |m| {
                m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
            }
        }

        const packet = try self.doOperation(.{ .Authenticate = .{
            .username = username,
            .password = password,
        } });
        defer Packet.free(self.allocator, packet);

        const result = switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    if (self.metrics) |m| {
                        m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
                    }
                    return ClientError.InvalidResponse;
                }
                if (reply.data) |data| {
                    break :blk try parseAuthResult(self.allocator, data);
                }
                if (self.metrics) |m| {
                    m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
                }
                return ClientError.InvalidResponse;
            },
            else => {
                if (self.metrics) |m| {
                    m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
                }
                return ClientError.InvalidResponse;
            },
        };

        if (self.metrics) |m| {
            m.recordOperation(.authenticate, timer.elapsedUs(), .success);
        }
        return result;
    }

    /// Authenticate with API key
    /// Returns AuthResult with session_id, username, and role
    /// Caller owns returned AuthResult and must call deinit() to free memory
    pub fn authenticateApiKey(self: *Self, api_key: []const u8) !AuthResult {
        const timer = Timer.start();
        errdefer {
            if (self.metrics) |m| {
                m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
            }
        }

        const packet = try self.doOperation(.{ .AuthenticateApiKey = .{
            .api_key = api_key,
        } });
        defer Packet.free(self.allocator, packet);

        const result = switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    if (self.metrics) |m| {
                        m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
                    }
                    return ClientError.InvalidResponse;
                }
                if (reply.data) |data| {
                    break :blk try parseAuthResult(self.allocator, data);
                }
                if (self.metrics) |m| {
                    m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
                }
                return ClientError.InvalidResponse;
            },
            else => {
                if (self.metrics) |m| {
                    m.recordOperation(.authenticate, timer.elapsedUs(), .failure);
                }
                return ClientError.InvalidResponse;
            },
        };

        if (self.metrics) |m| {
            m.recordOperation(.authenticate, timer.elapsedUs(), .success);
        }
        return result;
    }

    /// Logout and revoke the current session
    pub fn logout(self: *Self) !void {
        const timer = Timer.start();
        errdefer {
            if (self.metrics) |m| {
                m.recordOperation(.logout, timer.elapsedUs(), .failure);
            }
        }

        const packet = try self.doOperation(.Logout);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    if (self.metrics) |m| {
                        m.recordOperation(.logout, timer.elapsedUs(), .failure);
                    }
                    return ClientError.InvalidResponse;
                }
            },
            else => {
                if (self.metrics) |m| {
                    m.recordOperation(.logout, timer.elapsedUs(), .failure);
                }
                return ClientError.InvalidResponse;
            },
        }

        if (self.metrics) |m| {
            m.recordOperation(.logout, timer.elapsedUs(), .success);
        }
    }

    // ============================================================================
    // Generic Entity Management Operations
    // ============================================================================

    /// Generic create operation - creates Space, Store, Index, etc.
    /// Accepts Space, Store, Index structs directly and determines DocType automatically
    pub fn create(self: *Self, value: anytype) !void {
        const T = @TypeOf(value);

        // Determine DocType from the struct type
        const doc_type: proto.DocType = if (T == proto.Space)
            .Space
        else if (T == proto.Store)
            .Store
        else if (T == proto.Index)
            .Index
        else if (T == proto.User)
            .User
        else
            .Document; // Default to Document for other types

        // Get namespace from the value (User uses username instead of ns)
        const ns = if (T == proto.User) value.username else value.ns;

        // Encode value to BSON
        const bson_mod = @import("bson");
        var encoder = bson_mod.Encoder.init(self.allocator);
        defer encoder.deinit();
        const payload = try encoder.encode(value);
        defer self.allocator.free(payload);

        const op = proto.Operation{ .Create = .{
            .doc_type = doc_type,
            .ns = ns,
            .payload = payload,
            .auto_create = true,
            .metadata = null,
        } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    std.debug.print("Create operation failed with status: {any}\n", .{reply.status});
                    if (reply.data) |data| {
                        std.debug.print("Server error: {s}\n", .{data});
                    } else {
                        std.debug.print("No error details provided by server\n", .{});
                    }
                    return ClientError.InvalidResponse;
                }
            },
            else => {
                std.debug.print("Received unexpected packet type: {any}\n", .{packet.op});
                return ClientError.InvalidResponse;
            },
        }
    }

    /// Generic drop operation - drops Space, Store, Index, etc. using DocType
    pub fn drop(self: *Self, doc_type: proto.DocType, name: []const u8) !void {
        const op = proto.Operation{ .Drop = .{
            .doc_type = doc_type,
            .name = name,
        } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.InvalidResponse;
                }
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Generic list operation - lists Spaces, Stores, Indexes, etc. using DocType
    /// Returns JSON array of entities
    /// Caller must free the returned slice
    pub fn list(self: *Self, doc_type: proto.DocType, ns: ?[]const u8) ![]const u8 {
        const op = proto.Operation{ .List = .{
            .doc_type = doc_type,
            .ns = ns,
            .limit = null,
            .offset = null,
        } };

        const packet = try self.doOperation(op);
        errdefer Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    Packet.free(self.allocator, packet);
                    return ClientError.InvalidResponse;
                }
                if (reply.data) |data| {
                    const result = try self.allocator.dupe(u8, data);
                    Packet.free(self.allocator, packet);
                    break :blk result;
                }
                Packet.free(self.allocator, packet);
                return error.NoData;
            },
            else => {
                Packet.free(self.allocator, packet);
                return ClientError.InvalidResponse;
            },
        };
    }
};

/// Parse backup metadata from JSON response
fn parseBackupMetadata(allocator: std.mem.Allocator, data: []const u8) !ShinyDbClient.BackupMetadata {
    // Parse JSON: {"backup_path":"...","timestamp":123,"size_bytes":456,"vlog_count":7,"entry_count":890}

    // Parse backup_path
    const path_prefix = "\"backup_path\":\"";
    const path_start_idx = std.mem.indexOf(u8, data, path_prefix) orelse return ClientError.InvalidResponse;
    const path_val_start = path_start_idx + path_prefix.len;
    const path_val_end = std.mem.indexOfPos(u8, data, path_val_start, "\"") orelse return ClientError.InvalidResponse;
    const backup_path = try allocator.dupe(u8, data[path_val_start..path_val_end]);
    errdefer allocator.free(backup_path);

    // Parse timestamp
    const ts_prefix = "\"timestamp\":";
    const ts_start_idx = std.mem.indexOf(u8, data, ts_prefix) orelse return ClientError.InvalidResponse;
    const ts_val_start = ts_start_idx + ts_prefix.len;
    var ts_val_end = ts_val_start;
    while (ts_val_end < data.len and data[ts_val_end] != ',' and data[ts_val_end] != '}') : (ts_val_end += 1) {}
    const timestamp = std.fmt.parseInt(i64, std.mem.trim(u8, data[ts_val_start..ts_val_end], " "), 10) catch return ClientError.InvalidResponse;

    // Parse size_bytes
    const size_prefix = "\"size_bytes\":";
    const size_start_idx = std.mem.indexOf(u8, data, size_prefix) orelse return ClientError.InvalidResponse;
    const size_val_start = size_start_idx + size_prefix.len;
    var size_val_end = size_val_start;
    while (size_val_end < data.len and data[size_val_end] != ',' and data[size_val_end] != '}') : (size_val_end += 1) {}
    const size_bytes = std.fmt.parseInt(u64, std.mem.trim(u8, data[size_val_start..size_val_end], " "), 10) catch return ClientError.InvalidResponse;

    // Parse vlog_count
    const vlog_prefix = "\"vlog_count\":";
    const vlog_start_idx = std.mem.indexOf(u8, data, vlog_prefix) orelse return ClientError.InvalidResponse;
    const vlog_val_start = vlog_start_idx + vlog_prefix.len;
    var vlog_val_end = vlog_val_start;
    while (vlog_val_end < data.len and data[vlog_val_end] != ',' and data[vlog_val_end] != '}') : (vlog_val_end += 1) {}
    const vlog_count = std.fmt.parseInt(u16, std.mem.trim(u8, data[vlog_val_start..vlog_val_end], " "), 10) catch return ClientError.InvalidResponse;

    // Parse entry_count
    const entry_prefix = "\"entry_count\":";
    const entry_start_idx = std.mem.indexOf(u8, data, entry_prefix) orelse return ClientError.InvalidResponse;
    const entry_val_start = entry_start_idx + entry_prefix.len;
    var entry_val_end = entry_val_start;
    while (entry_val_end < data.len and data[entry_val_end] != ',' and data[entry_val_end] != '}') : (entry_val_end += 1) {}
    const entry_count = std.fmt.parseInt(u64, std.mem.trim(u8, data[entry_val_start..entry_val_end], " "), 10) catch return ClientError.InvalidResponse;

    return ShinyDbClient.BackupMetadata{
        .backup_path = backup_path,
        .timestamp = timestamp,
        .size_bytes = size_bytes,
        .vlog_count = vlog_count,
        .entry_count = entry_count,
        .allocator = allocator,
    };
}

/// Parse authentication result from JSON response
fn parseAuthResult(allocator: std.mem.Allocator, data: []const u8) !ShinyDbClient.AuthResult {
    var result: ShinyDbClient.AuthResult = .{
        .session_id = &.{},
        .api_key = &.{},
        .username = &.{},
        .role = .none,
        .allocator = allocator,
    };

    // Parse session_id
    if (std.mem.indexOf(u8, data, "\"session_id\":\"")) |start| {
        const val_start = start + "\"session_id\":\"".len;
        if (std.mem.indexOfPos(u8, data, val_start, "\"")) |val_end| {
            result.session_id = try allocator.dupe(u8, data[val_start..val_end]);
        }
    }

    // Parse api_key
    if (std.mem.indexOf(u8, data, "\"api_key\":\"")) |start| {
        const val_start = start + "\"api_key\":\"".len;
        if (std.mem.indexOfPos(u8, data, val_start, "\"")) |val_end| {
            result.api_key = try allocator.dupe(u8, data[val_start..val_end]);
        }
    }

    // Parse username
    if (std.mem.indexOf(u8, data, "\"username\":\"")) |start| {
        const val_start = start + "\"username\":\"".len;
        if (std.mem.indexOfPos(u8, data, val_start, "\"")) |val_end| {
            result.username = try allocator.dupe(u8, data[val_start..val_end]);
        }
    }

    // Parse role
    if (std.mem.indexOf(u8, data, "\"role\":\"")) |start| {
        const val_start = start + "\"role\":\"".len;
        if (std.mem.indexOfPos(u8, data, val_start, "\"")) |val_end| {
            result.role = ShinyDbClient.Role.fromString(data[val_start..val_end]);
        }
    }

    return result;
}

// ============================================================================
// Unit Tests (inline for non-pub functions)
// ============================================================================

test "parseAuthResult — valid JSON with all fields" {
    const allocator = std.testing.allocator;
    const data = "{\"session_id\":\"sess_abc123\",\"api_key\":\"key_xyz789\",\"username\":\"admin_user\",\"role\":\"admin\"}";

    var result = try parseAuthResult(allocator, data);
    defer result.deinit();

    try std.testing.expectEqualStrings("sess_abc123", result.session_id);
    try std.testing.expectEqualStrings("key_xyz789", result.api_key);
    try std.testing.expectEqualStrings("admin_user", result.username);
    try std.testing.expectEqual(ShinyDbClient.Role.admin, result.role);
}

test "parseAuthResult — read_only role" {
    const allocator = std.testing.allocator;
    const data = "{\"session_id\":\"s1\",\"api_key\":\"k1\",\"username\":\"viewer\",\"role\":\"read_only\"}";

    var result = try parseAuthResult(allocator, data);
    defer result.deinit();

    try std.testing.expectEqualStrings("viewer", result.username);
    try std.testing.expectEqual(ShinyDbClient.Role.read_only, result.role);
}

test "parseAuthResult — partial JSON (missing fields)" {
    const allocator = std.testing.allocator;
    const data = "{\"username\":\"testuser\",\"role\":\"read_write\"}";

    var result = try parseAuthResult(allocator, data);
    defer result.deinit();

    // session_id and api_key should be empty slices (not allocated)
    try std.testing.expectEqual(@as(usize, 0), result.session_id.len);
    try std.testing.expectEqual(@as(usize, 0), result.api_key.len);
    try std.testing.expectEqualStrings("testuser", result.username);
    try std.testing.expectEqual(ShinyDbClient.Role.read_write, result.role);
}

test "parseBackupMetadata — valid JSON" {
    const allocator = std.testing.allocator;
    const data = "{\"backup_path\":\"/backups/db_20240101\",\"timestamp\":1704067200,\"size_bytes\":1048576,\"vlog_count\":3,\"entry_count\":50000}";

    var result = try parseBackupMetadata(allocator, data);
    defer result.deinit();

    try std.testing.expectEqualStrings("/backups/db_20240101", result.backup_path);
    try std.testing.expectEqual(@as(i64, 1704067200), result.timestamp);
    try std.testing.expectEqual(@as(u64, 1048576), result.size_bytes);
    try std.testing.expectEqual(@as(u16, 3), result.vlog_count);
    try std.testing.expectEqual(@as(u64, 50000), result.entry_count);
}

test "parseBackupMetadata — missing field returns error" {
    const allocator = std.testing.allocator;
    // Missing backup_path field
    const data = "{\"timestamp\":123,\"size_bytes\":456}";

    const result = parseBackupMetadata(allocator, data);
    try std.testing.expectError(error.InvalidResponse, result);
}
