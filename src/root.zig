const std = @import("std");
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
pub const proto = @import("proto");
const Operation = proto.Operation;
const Packet = proto.Packet;
const Status = proto.Status;
const BufferWriter = proto.BufferWriter;
const Attribute = proto.Attribute;
const common = @import("common.zig");
const milliTimestamp = common.milliTimestamp;

pub const ClientError = error{
    ConnectionFailed,
    SendFailed,
    ReceiveFailed,
    InvalidResponse,
    ServerError,
    NotFound,
    InvalidRequest,
    Unauthenticated,
    PermissionDenied,
};

// Response wrapper that owns both the packet and the payload buffer
const Response = struct {
    packet: Packet,
    payload_buffer: []u8,
    allocator: Allocator,

    pub fn free(self: *Response) void {
        Packet.free(self.allocator, self.packet);
        self.allocator.free(self.payload_buffer);
    }
};

// Re-export hierarchical client API
pub const ShinyDbClient = @import("shinydb_client.zig").ShinyDbClient;

// Re-export metrics module
pub const metrics = @import("metrics.zig");
pub const ClientMetrics = metrics.ClientMetrics;
pub const OperationType = metrics.OperationType;
pub const OperationResult = metrics.OperationResult;
pub const OperationStats = metrics.OperationStats;
pub const Timer = metrics.Timer;
pub const Logger = metrics.Logger;
pub const LogLevel = metrics.LogLevel;
pub const setLogLevel = metrics.setLogLevel;

// Re-export proto types for convenience
pub const FieldType = proto.FieldType;
pub const ValueType = proto.ValueType;
pub const DocType = proto.DocType;

// Re-export entity structs for management API
pub const Space = proto.Space;
pub const Store = proto.Store;
pub const Index = proto.Index;
pub const User = proto.User;
pub const Backup = proto.Backup;

// Re-export resilience/config types for testing
pub const RetryPolicy = @import("retry_policy.zig").RetryPolicy;
pub const CircuitBreaker = @import("circuit_breaker.zig").CircuitBreaker;
pub const TimeoutConfig = @import("timeout_config.zig").TimeoutConfig;
pub const client_error = @import("client_error.zig");

// YQL (shinydb Query Language)
pub const yql = @import("yql/mod.zig");
pub const ast = @import("yql/ast.zig");
pub const Query = @import("yql/builder.zig").Query;
pub const QueryResponse = @import("yql/builder.zig").QueryResponse;
