const std = @import("std");
const Io = std.Io;
const net = std.Io.net;
const proto = @import("proto");
const Packet = proto.Packet;
const Operation = proto.Operation;
const Buffer = @import("utils").Buffer;
const Mutex = @import("utils").Mutex;
const Now = @import("utils").Now;
const tls = @import("tls");

const ClientError = @import("client_error.zig").ClientError;
const RetryPolicy = @import("retry_policy.zig").RetryPolicy;
const CircuitBreaker = @import("circuit_breaker.zig").CircuitBreaker;
const TimeoutConfig = @import("timeout_config.zig").TimeoutConfig;

/// ShinyDbClient is the main entry point for connecting to shinydb.
/// It provides connection management, space-level operations, and is the base for SpaceClient/StoreClient.
pub const ShinyDbClient = struct {
    allocator: std.mem.Allocator,
    io: Io,
    socket: ?net.Stream,
    packet_id: u32,

    // Connection info (for reconnection)
    conn_str: ?[]const u8,

    // Retry policy
    retry_policy: RetryPolicy,

    // Circuit breaker
    circuit_breaker: CircuitBreaker,

    // Timeout configuration
    timeout_config: TimeoutConfig,

    // Pipelining state
    pending_requests: std.ArrayList(PendingRequest),

    // Reusable resources
    buffer_writer: Buffer,
    recv_buffer: std.ArrayList(u8),

    // Batched flush optimization
    flush_threshold: u32,
    sends_since_flush: u32,

    // Mutex for thread-safe access — serializes all operations on a single connection
    mutex: Mutex,

    // TLS state — valid only when tls_conn != null
    tls_conn: ?tls.Connection,
    tls_input_buf: [tls.input_buffer_len]u8,
    tls_output_buf: [tls.output_buffer_len]u8,
    tls_tcp_reader: net.Stream.Reader,
    tls_tcp_writer: net.Stream.Writer,

    const PendingRequest = struct {
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
            .packet_id = 0,
            .conn_str = null,
            .retry_policy = .{},
            .circuit_breaker = CircuitBreaker.init(5, 2, 30000),
            .timeout_config = TimeoutConfig.default,
            .pending_requests = .empty,
            .buffer_writer = try Buffer.init(allocator, 4 * 1024 * 1024),
            .recv_buffer = recv_buffer,
            .flush_threshold = flush_threshold,
            .sends_since_flush = 0,
            .mutex = .{},
            .tls_conn = null,
            .tls_input_buf = undefined,
            .tls_output_buf = undefined,
            .tls_tcp_reader = undefined,
            .tls_tcp_writer = undefined,
        };
        return client;
    }

    pub fn deinit(self: *Self) void {
        self.disconnect();
        if (self.conn_str) |s| self.allocator.free(s);
        self.pending_requests.deinit(self.allocator);
        self.buffer_writer.deinit();
        self.recv_buffer.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    /// Connect using a connection string: "host:port;uid=<uid>;key=<key>;tls=true|false"
    /// Performs TCP connect, optional TLS handshake, and authentication.
    /// Returns AuthResult — call auth.deinit() when done.
    pub fn connect(self: *Self, conn_str: []const u8) !AuthResult {
        const parsed = parseConnStr(conn_str) catch |err| {
            std.log.err("Invalid connection string: {}", .{err});
            return err;
        };

        // Store conn_str for reconnection (dupe before freeing old, in case conn_str IS self.conn_str)
        const new_conn_str = try self.allocator.dupe(u8, conn_str);
        if (self.conn_str) |old| self.allocator.free(old);
        self.conn_str = new_conn_str;

        // TCP connect
        const address = net.IpAddress.parseIp4(parsed.host, parsed.port) catch |err| {
            std.log.err("Failed to parse address {s}:{d}: {}", .{ parsed.host, parsed.port, err });
            return err;
        };
        const socket = address.connect(self.io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch |err| {
            std.log.err("TCP connection failed to {s}:{d}: {}", .{ parsed.host, parsed.port, err });
            return err;
        };
        self.socket = socket;
        self.tls_conn = null;

        std.log.debug("TCP connected to {s}:{d}", .{ parsed.host, parsed.port });

        // TLS handshake
        if (parsed.tls) {
            // Init tcp reader/writer backed by our stable heap-allocated buffers.
            // ShinyDbClient is heap-allocated, so these field addresses are stable.
            self.tls_tcp_reader = net.Stream.Reader.init(socket, self.io, &self.tls_input_buf);
            self.tls_tcp_writer = net.Stream.Writer.init(socket, self.io, &self.tls_output_buf);

            var rng_src: std.Random.IoSource = .{ .io = self.io };
            const rng = rng_src.interface();

            self.tls_conn = tls.client(
                &self.tls_tcp_reader.interface,
                &self.tls_tcp_writer.interface,
                .{
                    .rng = rng,
                    .host = parsed.host,
                    .root_ca = .{},
                    .insecure_skip_verify = true,
                    .now = std.Io.Timestamp.zero,
                },
            ) catch |err| {
                std.log.err("TLS handshake failed: {}", .{err});
                socket.close(self.io);
                self.socket = null;
                return err;
            };

            std.log.debug("TLS handshake complete", .{});
        }

        // Authenticate
        const auth = self.authenticate(parsed.uid, parsed.key) catch |err| {
            std.log.err("Authentication failed: {}", .{err});
            self.disconnect();
            return err;
        };

        std.log.info("Connected and authenticated to {s}:{d}", .{ parsed.host, parsed.port });
        return auth;
    }

    pub fn disconnect(self: *Self) void {
        if (self.tls_conn) |*tc| {
            tc.close() catch {};
            self.tls_conn = null;
        }
        if (self.socket) |*sock| {
            std.log.debug("Disconnecting", .{});
            // Use raw syscall to avoid panic on EBADF (can happen after connection reset)
            _ = std.posix.system.close(sock.socket.handle);
            self.socket = null;
        }
    }

    pub fn isConnected(self: *Self) bool {
        return self.socket != null;
    }

    pub fn reconnect(self: *Self) !void {
        self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        const stored = self.conn_str orelse {
            std.log.err("Cannot reconnect: no connection string stored", .{});
            return ClientError.ConnectionFailed;
        };
        // Dupe before disconnect so we don't lose it
        const conn_str_copy = try self.allocator.dupe(u8, stored);
        defer self.allocator.free(conn_str_copy);

        self.disconnect();

        const pending_count = self.pending_requests.items.len;
        if (pending_count > 0) {
            std.log.warn("Clearing {d} pending requests due to reconnection", .{pending_count});
        }
        self.pending_requests.clearRetainingCapacity();
        self.packet_id = 0;

        _ = try self.connect(conn_str_copy);
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

    pub fn sendAsync(self: *Self, op: Operation) !u64 {
        return self.sendAsyncWithTimeout(op, self.timeout_config.write_timeout_ms);
    }

    pub fn sendAsyncWithTimeout(self: *Self, op: Operation, timeout_ms: ?u32) !u64 {
        if (self.socket == null) {
            return ClientError.ConnectionFailed;
        }

        const start_time = (Now{ .io = self.io }).toMilliSeconds();
        const deadline: ?i64 = if (timeout_ms) |t| start_time + @as(i64, t) else null;

        const packet = Packet{
            .checksum = 0,
            .packet_length = 0,
            .packet_id = self.packet_id,
            .timestamp = (Now{ .io = self.io }).toMilliSeconds(),
            .op = op,
        };
        self.packet_id += 1;

        self.buffer_writer.reset();
        const serialized = try packet.serialize(&self.buffer_writer);

        var length_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &length_buf, @intCast(serialized.len), .little);

        if (self.tls_conn) |*tc| {
            // TLS path — writeAll encrypts and flushes each record automatically
            tc.writeAll(&length_buf) catch {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.WriteTimeout;
                }
                return ClientError.ConnectionReset;
            };
            tc.writeAll(serialized) catch {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.WriteTimeout;
                }
                return ClientError.ConnectionReset;
            };
        } else {
            // Plain TCP path
            var write_buffer: [64 * 1024]u8 = undefined;
            var writer = self.socket.?.writer(self.io, &write_buffer);

            writer.interface.writeAll(&length_buf) catch |err| {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.WriteTimeout;
                }
                if (err == error.WriteFailed) return ClientError.ConnectionReset;
                return err;
            };

            writer.interface.writeAll(serialized) catch |err| {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.WriteTimeout;
                }
                if (err == error.WriteFailed) return ClientError.ConnectionReset;
                return err;
            };

            writer.interface.flush() catch |err| {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.WriteTimeout;
                }
                if (err == error.WriteFailed) return ClientError.ConnectionReset;
                return err;
            };
        }

        if (deadline) |d| {
            if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.WriteTimeout;
        }

        try self.pending_requests.append(self.allocator, .{
            .packet_id = packet.packet_id,
            .timestamp = packet.timestamp,
        });

        return packet.packet_id;
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

        const start_time = (Now{ .io = self.io }).toMilliSeconds();
        const deadline: ?i64 = if (timeout_ms) |t| start_time + @as(i64, t) else null;

        var resp_length_buf: [4]u8 = undefined;

        if (self.tls_conn) |*tc| {
            // TLS path
            const header_n = tc.readAtLeast(&resp_length_buf, 4) catch {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
                }
                return ClientError.ConnectionReset;
            };
            if (header_n < 4) return ClientError.ConnectionReset;

            if (deadline) |d| {
                if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
            }

            const msg_len = std.mem.readInt(u32, &resp_length_buf, .little);

            if (msg_len > 256 * 1024 * 1024) {
                return ClientError.InvalidResponse;
            }

            if (self.recv_buffer.items.len < msg_len) {
                try self.recv_buffer.resize(self.allocator, msg_len);
            }

            const payload = self.recv_buffer.items[0..msg_len];

            const payload_n = tc.readAtLeast(payload, msg_len) catch {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
                }
                return ClientError.ConnectionReset;
            };
            if (payload_n < msg_len) return ClientError.ConnectionReset;

            if (deadline) |d| {
                if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
            }

            const packet = try Packet.deserialize(self.allocator, payload);
            _ = self.pending_requests.orderedRemove(0);

            if (self.recv_buffer.items.len > 64 * 1024) {
                self.recv_buffer.shrinkAndFree(self.allocator, 64 * 1024);
            }

            return packet;
        } else {
            // Plain TCP path
            var read_buffer: [64 * 1024]u8 = undefined;
            var reader = self.socket.?.reader(self.io, &read_buffer);

            reader.interface.readSliceAll(&resp_length_buf) catch |err| {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
                }
                if (err == error.EndOfStream or err == error.ReadFailed) {
                    return ClientError.ConnectionReset;
                }
                return err;
            };

            if (deadline) |d| {
                if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
            }

            const msg_len = std.mem.readInt(u32, &resp_length_buf, .little);

            if (msg_len > 256 * 1024 * 1024) {
                return ClientError.InvalidResponse;
            }

            if (self.recv_buffer.items.len < msg_len) {
                try self.recv_buffer.resize(self.allocator, msg_len);
            }

            const payload = self.recv_buffer.items[0..msg_len];
            reader.interface.readSliceAll(payload) catch |err| {
                if (deadline) |d| {
                    if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
                }
                if (err == error.EndOfStream or err == error.ReadFailed) {
                    return ClientError.ConnectionReset;
                }
                return err;
            };

            if (deadline) |d| {
                if ((Now{ .io = self.io }).toMilliSeconds() > d) return ClientError.ReadTimeout;
            }

            const packet = try Packet.deserialize(self.allocator, payload);
            _ = self.pending_requests.orderedRemove(0);

            if (self.recv_buffer.items.len > 64 * 1024) {
                self.recv_buffer.shrinkAndFree(self.allocator, 64 * 1024);
            }

            return packet;
        }
    }

    pub fn doOperation(self: *Self, op: Operation) !Packet {
        return self.doOperationWithTimeout(op, self.timeout_config.operation_timeout_ms);
    }

    pub fn doOperationWithTimeout(self: *Self, op: Operation, timeout_ms: ?u32) !Packet {
        self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        const start_time = (Now{ .io = self.io }).toMilliSeconds();
        const deadline: ?i64 = if (timeout_ms) |t| start_time + @as(i64, t) else null;

        const send_timeout = if (deadline) |d| blk: {
            const remaining = d - (Now{ .io = self.io }).toMilliSeconds();
            if (remaining <= 0) return ClientError.Timeout;
            break :blk @as(u32, @intCast(remaining));
        } else null;

        _ = try self.sendAsyncWithTimeout(op, send_timeout);

        const recv_timeout = if (deadline) |d| blk: {
            const remaining = d - (Now{ .io = self.io }).toMilliSeconds();
            if (remaining <= 0) return ClientError.Timeout;
            break :blk @as(u32, @intCast(remaining));
        } else null;

        return self.receiveAsyncWithTimeout(recv_timeout);
    }

    pub fn flushWrites(self: *Self) !void {
        if (self.socket == null) {
            return ClientError.ConnectionFailed;
        }
        // TLS auto-flushes on every writeAll — nothing extra needed
        if (self.tls_conn != null) return;

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

    pub fn ping(self: *Self) !void {
        if (!self.isConnected()) {
            return ClientError.ConnectionFailed;
        }

        const packet = try self.doOperation(.Flush);
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

    /// Flush memtable to disk to ensure data durability
    pub fn flush(self: *Self) !void {
        if (!self.isConnected()) {
            return ClientError.ConnectionFailed;
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
                    return ClientError.ServerError;
                }
            },
            else => |other| {
                std.debug.print("Flush returned unexpected operation: {}\n", .{other});
                return ClientError.InvalidResponse;
            },
        }
    }

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

    /// Authentication result returned by connect() and authenticate().
    pub const AuthResult = struct {
        token: [32]u8,
        /// Non-null when the server auto-regenerated the admin key on first login.
        /// Contains the new base64-encoded key. Caller must save this — it won't be shown again.
        new_key: ?[]const u8 = null,
        allocator: ?std.mem.Allocator = null,

        pub fn deinit(self: *AuthResult) void {
            if (self.new_key) |key| {
                if (self.allocator) |alloc| alloc.free(key);
                self.new_key = null;
            }
        }
    };

    /// Authenticate with uid and key. Returns AuthResult with session token.
    /// If the server auto-regenerated the default admin key, AuthResult.new_key
    /// contains the new key (base64). The caller must save it.
    pub fn authenticate(self: *Self, uid: []const u8, key: []const u8) !AuthResult {
        const packet = try self.doOperation(.{ .Authenticate = .{
            .uid = uid,
            .key = key,
        } });
        defer Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) return ClientError.InvalidResponse;
                if (reply.data) |data| {
                    if (data.len == 32) {
                        var token: [32]u8 = undefined;
                        @memcpy(&token, data[0..32]);
                        break :blk AuthResult{ .token = token };
                    } else if (data.len > 32) {
                        // 32 bytes token + new key (auto-regenerated)
                        var token: [32]u8 = undefined;
                        @memcpy(&token, data[0..32]);
                        const new_key = try self.allocator.dupe(u8, data[32..]);
                        break :blk AuthResult{ .token = token, .new_key = new_key, .allocator = self.allocator };
                    }
                    return ClientError.InvalidResponse;
                }
                return ClientError.InvalidResponse;
            },
            else => ClientError.InvalidResponse,
        };
    }

    /// Logout and revoke the current session
    pub fn logout(self: *Self) !void {
        const packet = try self.doOperation(.Logout);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    pub fn create(self: *Self, value: anytype) !void {
        const T = @TypeOf(value);

        const doc_type: proto.DocType = if (T == proto.Space)
            .Space
        else if (T == proto.Store)
            .Store
        else if (T == proto.Index)
            .Index
        else if (T == proto.User)
            .User
        else
            .Document;

        const ns = if (T == proto.User) value.username else value.ns;

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

    /// Create a user and return the server-generated key (base64).
    /// Caller must free the returned slice.
    pub fn createUser(self: *Self, username: []const u8, role: u8) ![]const u8 {
        const bson_mod = @import("bson");
        const user = proto.User{
            .id = 0,
            .username = username,
            .password_hash = "",
            .role = role,
        };
        var encoder = bson_mod.Encoder.init(self.allocator);
        defer encoder.deinit();
        const payload = try encoder.encode(user);
        defer self.allocator.free(payload);

        const op = proto.Operation{ .Create = .{
            .doc_type = .User,
            .ns = username,
            .payload = payload,
            .auto_create = true,
            .metadata = null,
        } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) return ClientError.InvalidResponse;
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    pub fn regenerateKey(self: *Self, username: []const u8) ![]const u8 {
        const op = proto.Operation{ .RegenerateKey = .{ .uid = username } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) return ClientError.InvalidResponse;
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    pub fn updateUser(self: *Self, username: []const u8, role: u8) !void {
        const op = proto.Operation{ .UpdateUser = .{ .uid = username, .role = role } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

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

    /// Shutdown the database server
    pub fn shutdown(self: *Self) !void {
        const op = proto.Operation.Shutdown;

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Get engine statistics as BSON bytes
    pub fn stats(self: *Self, stat: proto.StatsTag) ![]const u8 {
        const op = proto.Operation{ .Stats = .{ .stat = stat } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Garbage Collect the Value Logs by ids
    pub fn collect(self: *Self, vlog_ids: []const u8) ![]const u8 {
        const ids = try self.allocator.dupe(u8, vlog_ids);
        const op = proto.Operation{ .Collect = .{ .vlogs = ids } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    if (reply.data) |data| {
                        std.debug.print("Collect failed: {s}\n", .{data});
                    }
                    return ClientError.ServerError;
                }
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Backup Database, Indexes, Value Logs and Config
    pub fn backup(self: *Self, path: []const u8) ![]const u8 {
        const op = proto.Operation{ .Backup = .{ .path = path } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Restore Database, Indexes, Value Logs and Config
    pub fn restore(self: *Self, backup_path: []const u8, target_path: []const u8) ![]const u8 {
        const op = proto.Operation{ .Restore = .{ .backup_path = backup_path, .target_path = target_path } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Set server operation mode.
    pub fn setMode(self: *Self, online: bool) ![]const u8 {
        const op = proto.Operation{ .SetMode = .{ .online = online } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// List all vlog headers with storage stats
    pub fn listVlogs(self: *Self) ![]const u8 {
        const op = proto.Operation.Vlogs;

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Get server configuration as BSON bytes.
    /// Caller must free the returned slice.
    pub fn getConfig(self: *Self) ![]const u8 {
        const op = proto.Operation{ .GetConfig = {} };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
                if (reply.data) |data| {
                    return try self.allocator.dupe(u8, data);
                }
                return ClientError.InvalidResponse;
            },
            else => return ClientError.InvalidResponse,
        }
    }

    /// Set server configuration from BSON bytes.
    pub fn setConfig(self: *Self, bson_data: []const u8) !void {
        const duped = try self.allocator.dupe(u8, bson_data);
        const op = proto.Operation{ .SetConfig = .{ .data = duped } };

        const packet = try self.doOperation(op);
        defer Packet.free(self.allocator, packet);

        switch (packet.op) {
            .Reply => |reply| {
                if (reply.status != .ok) {
                    return ClientError.ServerError;
                }
            },
            else => return ClientError.InvalidResponse,
        }
    }
};

// ============================================================================
// Connection string parser
// ============================================================================

const ParsedConnStr = struct {
    host: []const u8,
    port: u16,
    uid: []const u8,
    key: []const u8,
    tls: bool,
};

fn parseConnStr(conn_str: []const u8) !ParsedConnStr {
    var host: []const u8 = "";
    var port: u16 = 0;
    var uid: []const u8 = "";
    var key: []const u8 = "";
    var tls_enabled: bool = false;

    var iter = std.mem.splitScalar(u8, conn_str, ';');

    // First segment: host:port
    const addr_part = iter.next() orelse return error.InvalidConnStr;
    const colon_idx = std.mem.lastIndexOfScalar(u8, addr_part, ':') orelse return error.InvalidConnStr;
    host = addr_part[0..colon_idx];
    port = std.fmt.parseInt(u16, addr_part[colon_idx + 1 ..], 10) catch return error.InvalidConnStr;

    // Remaining segments: key=value
    while (iter.next()) |pair| {
        const eq_idx = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        const k = pair[0..eq_idx];
        const v = pair[eq_idx + 1 ..];
        if (std.mem.eql(u8, k, "uid")) {
            uid = v;
        } else if (std.mem.eql(u8, k, "key")) {
            key = v;
        } else if (std.mem.eql(u8, k, "tls")) {
            tls_enabled = std.mem.eql(u8, v, "true");
        }
    }

    return .{ .host = host, .port = port, .uid = uid, .key = key, .tls = tls_enabled };
}

  

fn parseBackupMetadata(allocator: std.mem.Allocator, data: []const u8) !ShinyDbClient.BackupMetadata {
    const path_prefix = "\"backup_path\":\"";
    const path_start_idx = std.mem.indexOf(u8, data, path_prefix) orelse return error.InvalidResponse;
    const path_val_start = path_start_idx + path_prefix.len;
    const path_val_end = std.mem.indexOfPos(u8, data, path_val_start, "\"") orelse return error.InvalidResponse;
    const backup_path = try allocator.dupe(u8, data[path_val_start..path_val_end]);
    errdefer allocator.free(backup_path);

    const ts_prefix = "\"timestamp\":";
    const ts_start_idx = std.mem.indexOf(u8, data, ts_prefix) orelse return error.InvalidResponse;
    const ts_val_start = ts_start_idx + ts_prefix.len;
    var ts_val_end = ts_val_start;
    while (ts_val_end < data.len and data[ts_val_end] != ',' and data[ts_val_end] != '}') : (ts_val_end += 1) {}
    const timestamp = std.fmt.parseInt(i64, std.mem.trim(u8, data[ts_val_start..ts_val_end], " "), 10) catch return error.InvalidResponse;

    const size_prefix = "\"size_bytes\":";
    const size_start_idx = std.mem.indexOf(u8, data, size_prefix) orelse return error.InvalidResponse;
    const size_val_start = size_start_idx + size_prefix.len;
    var size_val_end = size_val_start;
    while (size_val_end < data.len and data[size_val_end] != ',' and data[size_val_end] != '}') : (size_val_end += 1) {}
    const size_bytes = std.fmt.parseInt(u64, std.mem.trim(u8, data[size_val_start..size_val_end], " "), 10) catch return error.InvalidResponse;

    const vlog_prefix = "\"vlog_count\":";
    const vlog_start_idx = std.mem.indexOf(u8, data, vlog_prefix) orelse return error.InvalidResponse;
    const vlog_val_start = vlog_start_idx + vlog_prefix.len;
    var vlog_val_end = vlog_val_start;
    while (vlog_val_end < data.len and data[vlog_val_end] != ',' and data[vlog_val_end] != '}') : (vlog_val_end += 1) {}
    const vlog_count = std.fmt.parseInt(u16, std.mem.trim(u8, data[vlog_val_start..vlog_val_end], " "), 10) catch return error.InvalidResponse;

    const entry_prefix = "\"entry_count\":";
    const entry_start_idx = std.mem.indexOf(u8, data, entry_prefix) orelse return error.InvalidResponse;
    const entry_val_start = entry_start_idx + entry_prefix.len;
    var entry_val_end = entry_val_start;
    while (entry_val_end < data.len and data[entry_val_end] != ',' and data[entry_val_end] != '}') : (entry_val_end += 1) {}
    const entry_count = std.fmt.parseInt(u64, std.mem.trim(u8, data[entry_val_start..entry_val_end], " "), 10) catch return error.InvalidResponse;

    return ShinyDbClient.BackupMetadata{
        .backup_path = backup_path,
        .timestamp = timestamp,
        .size_bytes = size_bytes,
        .vlog_count = vlog_count,
        .entry_count = entry_count,
        .allocator = allocator,
    };
}

test "parseConnStr — full connection string" {
    const conn_str = "127.0.0.1:23469;uid=admin;key=NH8ohl2LHDT8xSJbHGPAsCluCh5pe8Ldn+hckcJovXk=;tls=true";
    const parsed = try parseConnStr(conn_str);
    try std.testing.expectEqualStrings("127.0.0.1", parsed.host);
    try std.testing.expectEqual(@as(u16, 23469), parsed.port);
    try std.testing.expectEqualStrings("admin", parsed.uid);
    try std.testing.expectEqualStrings("NH8ohl2LHDT8xSJbHGPAsCluCh5pe8Ldn+hckcJovXk=", parsed.key);
    try std.testing.expect(parsed.tls);
}

test "parseConnStr — tls=false" {
    const conn_str = "127.0.0.1:5432;uid=user;key=secret;tls=false";
    const parsed = try parseConnStr(conn_str);
    try std.testing.expect(!parsed.tls);
}

test "parseConnStr — no tls field" {
    const conn_str = "127.0.0.1:5432;uid=user;key=secret";
    const parsed = try parseConnStr(conn_str);
    try std.testing.expect(!parsed.tls);
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
    const data = "{\"timestamp\":123,\"size_bytes\":456}";
    const result = parseBackupMetadata(allocator, data);
    try std.testing.expectError(error.InvalidResponse, result);
}
