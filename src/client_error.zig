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
