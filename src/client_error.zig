const std = @import("std");

pub const ClientError = error{
    // Connection errors (transient - can retry)
    ConnectionFailed,
    ConnectionReset,
    ConnectionRefused,
    NetworkError,

    // Timeout errors (transient - can retry)
    Timeout,
    ReadTimeout,
    WriteTimeout,

    // Protocol errors (permanent - don't retry)
    InvalidResponse,
    InvalidRequest,
    ProtocolError,

    // Resource errors (transient - can retry)
    PipelineFull,
    BufferOverflow,

    // Server errors
    ServerError, // Generic server error (may retry)
    ServiceUnavailable, // Server overloaded (should retry)
    NotFound, // Resource not found (don't retry)
    PermissionDenied, // Auth failure (don't retry)
};

/// Standard error codes (matching proto ErrorCode enum).
/// Sent in Reply data payload as JSON: {"code":1101,"error":"store_not_found","message":"..."}
pub const ErrorCode = enum(u16) {
    success = 1000,
    not_found = 1100,
    store_not_found = 1101,
    space_not_found = 1102,
    user_not_found = 1103,
    index_not_found = 1104,
    key_not_found = 1105,
    unauthorized = 1200,
    invalid_credentials = 1201,
    session_expired = 1202,
    permission_denied = 1203,
    account_locked = 1204,
    user_disabled = 1205,
    user_already_exists = 1206,
    invalid_request = 1300,
    invalid_namespace = 1301,
    invalid_query = 1302,
    key_too_large = 1303,
    document_too_large = 1304,
    batch_too_large = 1305,
    no_index_on_field = 1306,
    invalid_field_type = 1307,
    missing_required_field = 1308,
    duplicate_index = 1309,
    server_offline = 1400,
    not_leader = 1401,
    read_only = 1402,
    store_already_exists = 1403,
    internal_error = 1500,
    io_error = 1501,
    wal_error = 1502,
    replication_error = 1503,
};
