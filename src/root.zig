const std = @import("std");
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
pub const proto = @import("proto");
pub const bson = @import("bson");
const Operation = proto.Operation;
const Packet = proto.Packet;
const Status = proto.Status;
const Attribute = proto.Attribute;

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

// Re-export proto types for convenience
pub const FieldType = proto.FieldType;
pub const ValueType = proto.ValueType;
pub const DocType = proto.DocType;
pub const StatsTag = proto.StatsTag;

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

// Schema validation
pub const schema = @import("schema.zig");
pub const Schema = schema.Schema;
pub const SchemaFieldType = schema.FieldType;
pub const FieldRule = schema.FieldRule;
pub const ValidationError = schema.ValidationError;
pub const FieldError = schema.FieldError;
