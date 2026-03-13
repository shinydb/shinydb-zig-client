const std = @import("std");
const Allocator = std.mem.Allocator;
const bson = @import("bson");

/// Schema field types for runtime validation — aligned with BSON spec types.
pub const FieldType = enum {
    string,
    int,        // any Zig integer type (stored as BSON int64)
    int32,      // 32-bit signed integer with range check
    double,     // alias for float (BSON double)
    float,      // 64-bit IEEE 754 float (kept for backward compat)
    boolean,
    date,       // i64 millisecond timestamp (BSON datetime)
    object_id,  // [12]u8 or []const u8 of length 12
    array,
    object,     // embedded struct / BSON document
    binary,     // []const u8 / []u8 raw bytes
    decimal128, // [16]u8 (BSON Decimal128 bytes)
    null_type,  // optional types that must be null
    uuid,       // [16]u8 (RFC 4122 UUID bytes)
    timestamp,  // u64 or struct { increment: u32, timestamp: u32 } (BSON internal)
};

/// Validation rule for a single field.
pub const FieldRule = struct {
    field_type: FieldType = .string,
    required: bool = false,
    min: ?f64 = null,
    max: ?f64 = null,
    min_length: ?usize = null,
    max_length: ?usize = null,
    enum_values: ?[]const []const u8 = null,
};

/// A single validation error.
pub const FieldError = struct {
    field: []const u8,
    message: []const u8,
};

/// ValidationError holds all field errors from a validation pass.
pub const ValidationError = struct {
    errors: []const FieldError,
    allocator: Allocator,

    pub fn deinit(self: *ValidationError) void {
        self.allocator.free(self.errors);
    }

    pub fn format(self: ValidationError, allocator: Allocator) ![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        try buf.appendSlice(allocator, "Validation failed: ");
        for (self.errors, 0..) |err, i| {
            if (i > 0) try buf.appendSlice(allocator, "; ");
            try buf.appendSlice(allocator, err.field);
            try buf.appendSlice(allocator, ": ");
            try buf.appendSlice(allocator, err.message);
        }
        return try buf.toOwnedSlice(allocator);
    }
};

/// Comptime schema definition.
/// Usage:
/// ```
/// const UserSchema = Schema(&.{
///     .{ "name",    .{ .field_type = .string, .required = true, .min_length = 1, .max_length = 100 } },
///     .{ "email",   .{ .field_type = .string, .required = true } },
///     .{ "age",     .{ .field_type = .int32, .min = 0, .max = 150 } },
///     .{ "score",   .{ .field_type = .double, .min = 0, .max = 100 } },
///     .{ "created", .{ .field_type = .date } },
///     .{ "id",      .{ .field_type = .object_id } },
///     .{ "price",   .{ .field_type = .decimal128 } },
///     .{ "uid",     .{ .field_type = .uuid } },
///     .{ "role",    .{ .field_type = .string, .enum_values = &.{ "admin", "user", "guest" } } },
/// });
/// ```
pub fn Schema(comptime fields: []const struct { []const u8, FieldRule }) type {
    return struct {
        const Self = @This();
        pub const field_defs = fields;

        /// Validate a Zig struct at compile time (type checking) and runtime (value checking).
        /// Returns a list of FieldErrors or null if valid.
        pub fn validate(allocator: Allocator, value: anytype) !?ValidationError {
            const T = @TypeOf(value);
            const info = @typeInfo(T);

            if (info != .@"struct") {
                @compileError("Schema.validate expects a struct, got " ++ @typeName(T));
            }

            var errors: std.ArrayList(FieldError) = .empty;
            errdefer errors.deinit(allocator);

            inline for (fields) |entry| {
                const name = entry[0];
                const rule = entry[1];

                if (@hasField(T, name)) {
                    const field_value = @field(value, name);
                    try validateField(allocator, name, rule, field_value, &errors);
                } else if (rule.required) {
                    try errors.append(allocator, .{ .field = name, .message = "is required" });
                }
            }

            if (errors.items.len > 0) {
                return ValidationError{
                    .errors = try errors.toOwnedSlice(allocator),
                    .allocator = allocator,
                };
            }
            errors.deinit(allocator);
            return null;
        }

        /// Validate and encode to BSON in one step. Returns error if validation fails.
        pub fn validateAndEncode(allocator: Allocator, value: anytype) ![]const u8 {
            if (try validate(allocator, value)) |*verr| {
                var ve = verr.*;
                ve.deinit();
                return error.ValidationFailed;
            }

            var encoder = try bson.Encoder.initWithCapacity(allocator, 4096);
            defer encoder.deinit();
            return try encoder.encodeToOwned(value);
        }
    };
}

fn validateField(allocator: Allocator, name: []const u8, rule: FieldRule, value: anytype, errors: *std.ArrayList(FieldError)) !void {
    const T = @TypeOf(value);
    const info = @typeInfo(T);

    // Handle optional fields
    if (info == .optional) {
        if (value == null) {
            if (rule.required) {
                try errors.append(allocator, .{ .field = name, .message = "is required" });
            }
            return;
        }
        // Unwrap and validate the inner value
        try validateField(allocator, name, rule, value.?, errors);
        return;
    }

    // Type checking — use comptime guards around type-specific casts
    // since all switch branches are compiled for every instantiation.
    switch (rule.field_type) {
        .string => {
            if (!isStringType(T)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'string'" });
                return;
            }
            if (comptime isStringType(T)) {
                const str: []const u8 = asSlice(value);

                // String length checks
                if (rule.min_length) |ml| {
                    if (str.len < ml) {
                        try errors.append(allocator, .{ .field = name, .message = "too short" });
                    }
                }
                if (rule.max_length) |ml| {
                    if (str.len > ml) {
                        try errors.append(allocator, .{ .field = name, .message = "too long" });
                    }
                }

                // Enum check
                if (rule.enum_values) |allowed| {
                    var found = false;
                    for (allowed) |ev| {
                        if (std.mem.eql(u8, str, ev)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        try errors.append(allocator, .{ .field = name, .message = "not in allowed values" });
                    }
                }
            }
        },
        .int => {
            if (!isIntType(T)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'int'" });
                return;
            }
            if (comptime isIntType(T)) {
                const num: f64 = @floatFromInt(value);
                if (rule.min) |m| {
                    if (num < m) {
                        try errors.append(allocator, .{ .field = name, .message = "below minimum" });
                    }
                }
                if (rule.max) |m| {
                    if (num > m) {
                        try errors.append(allocator, .{ .field = name, .message = "above maximum" });
                    }
                }
            }
        },
        .int32 => {
            if (!isIntType(T)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'int32'" });
                return;
            }
            if (comptime isIntType(T)) {
                const num: f64 = @floatFromInt(value);
                // Range check for i32
                if (num < -2147483648.0 or num > 2147483647.0) {
                    try errors.append(allocator, .{ .field = name, .message = "out of int32 range" });
                }
                if (rule.min) |m| {
                    if (num < m) {
                        try errors.append(allocator, .{ .field = name, .message = "below minimum" });
                    }
                }
                if (rule.max) |m| {
                    if (num > m) {
                        try errors.append(allocator, .{ .field = name, .message = "above maximum" });
                    }
                }
            }
        },
        .float, .double => {
            if (!isFloatType(T)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'double'" });
                return;
            }
            if (comptime isFloatType(T)) {
                const num: f64 = @floatCast(value);
                if (rule.min) |m| {
                    if (num < m) {
                        try errors.append(allocator, .{ .field = name, .message = "below minimum" });
                    }
                }
                if (rule.max) |m| {
                    if (num > m) {
                        try errors.append(allocator, .{ .field = name, .message = "above maximum" });
                    }
                }
            }
        },
        .boolean => {
            if (T != bool) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'boolean'" });
            }
        },
        .date => {
            // BSON datetime is stored as i64 milliseconds since epoch
            if (!isIntType(T)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'date' (i64 ms timestamp)" });
                return;
            }
            if (comptime isIntType(T)) {
                const num: f64 = @floatFromInt(value);
                if (rule.min) |m| {
                    if (num < m) {
                        try errors.append(allocator, .{ .field = name, .message = "below minimum" });
                    }
                }
                if (rule.max) |m| {
                    if (num > m) {
                        try errors.append(allocator, .{ .field = name, .message = "above maximum" });
                    }
                }
            }
        },
        .object_id => {
            // Expect [12]u8 or a slice of length 12
            if (comptime isFixedArray(T, 12)) {
                // ok — [12]u8
            } else if (comptime isStringType(T)) {
                const str: []const u8 = asSlice(value);
                if (str.len != 12) {
                    try errors.append(allocator, .{ .field = name, .message = "objectId must be 12 bytes" });
                }
            } else {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'object_id' ([12]u8)" });
            }
        },
        .decimal128 => {
            // Expect [16]u8
            if (!comptime isFixedArray(T, 16)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'decimal128' ([16]u8)" });
            }
        },
        .uuid => {
            // Expect [16]u8
            if (!comptime isFixedArray(T, 16)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'uuid' ([16]u8)" });
            }
        },
        .null_type => {
            // For comptime this only makes sense with optionals (handled above).
            // If we get here with a non-null value, it's an error.
            try errors.append(allocator, .{ .field = name, .message = "expected null" });
        },
        .timestamp => {
            // Accept u64 or i64
            if (!isIntType(T)) {
                try errors.append(allocator, .{ .field = name, .message = "expected type 'timestamp' (u64)" });
            }
        },
        .array, .object, .binary => {},
    }
}

fn isStringType(comptime T: type) bool {
    if (T == []const u8 or T == []u8) return true;
    const info = @typeInfo(T);
    if (info == .pointer) {
        if (info.pointer.size == .many or info.pointer.size == .slice) {
            return info.pointer.child == u8;
        }
        // Handle *const [N:0]u8 (string literals) and *const [N]u8
        if (info.pointer.size == .one) {
            const child_info = @typeInfo(info.pointer.child);
            if (child_info == .array and child_info.array.child == u8) return true;
        }
    }
    return false;
}

fn isIntType(comptime T: type) bool {
    return @typeInfo(T) == .int or @typeInfo(T) == .comptime_int;
}

fn isFloatType(comptime T: type) bool {
    return @typeInfo(T) == .float or @typeInfo(T) == .comptime_float;
}

fn isFixedArray(comptime T: type, comptime expected_len: usize) bool {
    const info = @typeInfo(T);
    if (info == .array) {
        return info.array.child == u8 and info.array.len == expected_len;
    }
    return false;
}

/// Coerce any string-like type to []const u8.
fn asSlice(value: anytype) []const u8 {
    const T = @TypeOf(value);
    const info = @typeInfo(T);
    if (T == []const u8 or T == []u8) return value;
    if (info == .pointer and info.pointer.size == .one) {
        // *const [N:0]u8 or *const [N]u8 → slice
        return value;
    }
    return value;
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

const UserSchema = Schema(&.{
    .{ "name", .{ .field_type = .string, .required = true, .min_length = 1, .max_length = 100 } },
    .{ "email", .{ .field_type = .string, .required = true } },
    .{ "age", .{ .field_type = .int, .min = 0, .max = 150 } },
    .{ "role", .{ .field_type = .string, .enum_values = &.{ "admin", "user", "guest" } } },
    .{ "active", .{ .field_type = .boolean } },
});

test "schema - valid struct passes" {
    const user = .{
        .name = "Alice",
        .email = "alice@example.com",
        .age = @as(i32, 30),
        .role = "user",
        .active = true,
    };

    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "schema - missing required field" {
    const user = .{
        .email = "alice@example.com",
        .age = @as(i32, 30),
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    // "name" is required but missing from struct
    var found_name = false;
    for (verr.errors) |err| {
        if (std.mem.eql(u8, err.field, "name")) found_name = true;
    }
    try testing.expect(found_name);
}

test "schema - string too short" {
    const user = .{
        .name = "",
        .email = "a@b.com",
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    var found = false;
    for (verr.errors) |err| {
        if (std.mem.eql(u8, err.field, "name") and std.mem.eql(u8, err.message, "too short")) found = true;
    }
    try testing.expect(found);
}

test "schema - string too long" {
    const long_name = "A" ** 101;
    const user = .{
        .name = @as([]const u8, long_name),
        .email = "a@b.com",
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    var found = false;
    for (verr.errors) |err| {
        if (std.mem.eql(u8, err.field, "name") and std.mem.eql(u8, err.message, "too long")) found = true;
    }
    try testing.expect(found);
}

test "schema - int below minimum" {
    const user = .{
        .name = "Bob",
        .email = "bob@b.com",
        .age = @as(i32, -5),
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    var found = false;
    for (verr.errors) |err| {
        if (std.mem.eql(u8, err.field, "age") and std.mem.eql(u8, err.message, "below minimum")) found = true;
    }
    try testing.expect(found);
}

test "schema - int above maximum" {
    const user = .{
        .name = "Bob",
        .email = "bob@b.com",
        .age = @as(i32, 200),
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    var found = false;
    for (verr.errors) |err| {
        if (std.mem.eql(u8, err.field, "age") and std.mem.eql(u8, err.message, "above maximum")) found = true;
    }
    try testing.expect(found);
}

test "schema - enum value not in allowed list" {
    const user = .{
        .name = "Charlie",
        .email = "c@c.com",
        .role = "superadmin",
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    var found = false;
    for (verr.errors) |err| {
        if (std.mem.eql(u8, err.field, "role") and std.mem.eql(u8, err.message, "not in allowed values")) found = true;
    }
    try testing.expect(found);
}

test "schema - multiple errors collected" {
    const user = .{
        .name = "",
        .email = "a@b.com",
        .age = @as(i32, -1),
        .role = "superadmin",
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    // Should have at least 3 errors: name too short, age below min, role not in enum
    try testing.expect(verr.errors.len >= 3);
}

test "schema - format error message" {
    const user = .{
        .name = "",
        .email = "a@b.com",
    };

    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    const msg = try verr.format(testing.allocator);
    defer testing.allocator.free(msg);

    try testing.expect(std.mem.startsWith(u8, msg, "Validation failed: "));
    try testing.expect(std.mem.indexOf(u8, msg, "name: too short") != null);
}

test "schema - optional field with null passes" {
    const OptSchema = Schema(&.{
        .{ "name", .{ .field_type = .string, .required = true } },
        .{ "bio", .{ .field_type = .string, .required = false } },
    });

    const user = .{
        .name = "Alice",
        .bio = @as(?[]const u8, null),
    };

    const result = try OptSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "schema - required optional field with null fails" {
    const OptSchema = Schema(&.{
        .{ "name", .{ .field_type = .string, .required = true } },
    });

    const user = .{
        .name = @as(?[]const u8, null),
    };

    var verr = (try OptSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();

    try testing.expect(verr.errors.len == 1);
    try testing.expectEqualStrings("name", verr.errors[0].field);
    try testing.expectEqualStrings("is required", verr.errors[0].message);
}

test "schema - float validation" {
    const PriceSchema = Schema(&.{
        .{ "price", .{ .field_type = .float, .required = true, .min = 0, .max = 99999 } },
    });

    // Valid
    const valid = .{ .price = @as(f64, 29.99) };
    const r1 = try PriceSchema.validate(testing.allocator, valid);
    try testing.expect(r1 == null);

    // Below min
    const invalid = .{ .price = @as(f64, -1.0) };
    var verr = (try PriceSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("below minimum", verr.errors[0].message);
}

test "schema - float above maximum" {
    const PriceSchema = Schema(&.{
        .{ "price", .{ .field_type = .float, .required = true, .min = 0, .max = 99999 } },
    });

    const invalid = .{ .price = @as(f64, 100000.0) };
    var verr = (try PriceSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("above maximum", verr.errors[0].message);
}

test "schema - boolean field passes" {
    const BoolSchema = Schema(&.{
        .{ "active", .{ .field_type = .boolean, .required = true } },
    });

    const valid_true = .{ .active = true };
    const r1 = try BoolSchema.validate(testing.allocator, valid_true);
    try testing.expect(r1 == null);

    const valid_false = .{ .active = false };
    const r2 = try BoolSchema.validate(testing.allocator, valid_false);
    try testing.expect(r2 == null);
}

test "schema - valid enum value passes" {
    const user = .{
        .name = "Alice",
        .email = "alice@example.com",
        .role = "admin",
    };

    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "schema - non-required missing field passes" {
    // age, role, active are not required — omitting them should be fine
    const user = .{
        .name = "Alice",
        .email = "alice@example.com",
    };

    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "schema - int at exact boundary passes" {
    const user = .{
        .name = "Bob",
        .email = "bob@b.com",
        .age = @as(i32, 0),
    };
    const r1 = try UserSchema.validate(testing.allocator, user);
    try testing.expect(r1 == null);

    const user2 = .{
        .name = "Bob",
        .email = "bob@b.com",
        .age = @as(i32, 150),
    };
    const r2 = try UserSchema.validate(testing.allocator, user2);
    try testing.expect(r2 == null);
}

test "schema - string at exact length boundary passes" {
    const exact_name = "A" ** 100;
    const user = .{
        .name = @as([]const u8, exact_name),
        .email = "a@b.com",
    };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "schema - single char name passes min_length 1" {
    const user = .{
        .name = "X",
        .email = "x@y.com",
    };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "schema - validateAndEncode valid struct returns BSON" {
    const user = .{
        .name = @as([]const u8, "Alice"),
        .email = @as([]const u8, "alice@example.com"),
        .age = @as(i32, 30),
        .role = @as([]const u8, "user"),
        .active = true,
    };

    const encoded = try UserSchema.validateAndEncode(testing.allocator, user);
    defer testing.allocator.free(encoded);

    // BSON starts with a 4-byte little-endian length
    try testing.expect(encoded.len > 4);
    const doc_len = std.mem.readInt(i32, encoded[0..4], .little);
    try testing.expect(doc_len == @as(i32, @intCast(encoded.len)));
}

test "schema - validateAndEncode invalid struct returns error" {
    // Missing required 'name'
    const user = .{
        .email = @as([]const u8, "alice@example.com"),
    };

    const result = UserSchema.validateAndEncode(testing.allocator, user);
    try testing.expectError(error.ValidationFailed, result);
}

test "schema - multiple required fields missing" {
    const StrictSchema = Schema(&.{
        .{ "a", .{ .field_type = .string, .required = true } },
        .{ "b", .{ .field_type = .string, .required = true } },
        .{ "c", .{ .field_type = .int, .required = true } },
    });

    const empty = .{
        .x = "unrelated",
    };

    var verr = (try StrictSchema.validate(testing.allocator, empty)).?;
    defer verr.deinit();

    try testing.expect(verr.errors.len == 3);
}

test "schema - optional field with value validates normally" {
    const OptSchema = Schema(&.{
        .{ "bio", .{ .field_type = .string, .required = false, .min_length = 5 } },
    });

    // Present but too short
    const user = .{
        .bio = @as(?[]const u8, "hi"),
    };

    var verr = (try OptSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("too short", verr.errors[0].message);
}

// ── New BSON type tests ──

test "schema - int32 valid range passes" {
    const CountSchema = Schema(&.{
        .{ "count", .{ .field_type = .int32, .required = true, .min = 0, .max = 100 } },
    });

    const valid = .{ .count = @as(i32, 50) };
    const result = try CountSchema.validate(testing.allocator, valid);
    try testing.expect(result == null);
}

test "schema - int32 below minimum fails" {
    const CountSchema = Schema(&.{
        .{ "count", .{ .field_type = .int32, .min = 0 } },
    });

    const invalid = .{ .count = @as(i32, -1) };
    var verr = (try CountSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("below minimum", verr.errors[0].message);
}

test "schema - int32 out of range fails" {
    const CountSchema = Schema(&.{
        .{ "count", .{ .field_type = .int32 } },
    });

    // i64 value that exceeds i32 range
    const invalid = .{ .count = @as(i64, 2147483648) };
    var verr = (try CountSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("out of int32 range", verr.errors[0].message);
}

test "schema - double validation passes" {
    const ScoreSchema = Schema(&.{
        .{ "score", .{ .field_type = .double, .required = true, .min = 0, .max = 100 } },
    });

    const valid = .{ .score = @as(f64, 95.5) };
    const result = try ScoreSchema.validate(testing.allocator, valid);
    try testing.expect(result == null);
}

test "schema - double below minimum fails" {
    const ScoreSchema = Schema(&.{
        .{ "score", .{ .field_type = .double, .min = 0 } },
    });

    const invalid = .{ .score = @as(f64, -0.1) };
    var verr = (try ScoreSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("below minimum", verr.errors[0].message);
}

test "schema - date field accepts i64" {
    const EventSchema = Schema(&.{
        .{ "created_at", .{ .field_type = .date, .required = true } },
    });

    const valid = .{ .created_at = @as(i64, 1710000000000) };
    const result = try EventSchema.validate(testing.allocator, valid);
    try testing.expect(result == null);
}

test "schema - date field with min/max" {
    const EventSchema = Schema(&.{
        .{ "ts", .{ .field_type = .date, .min = 0, .max = 4102444800000 } }, // max = 2100-01-01
    });

    const valid = .{ .ts = @as(i64, 1710000000000) };
    const r1 = try EventSchema.validate(testing.allocator, valid);
    try testing.expect(r1 == null);

    const invalid = .{ .ts = @as(i64, -1) };
    var verr = (try EventSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("below minimum", verr.errors[0].message);
}

test "schema - object_id accepts [12]u8" {
    const DocSchema = Schema(&.{
        .{ "id", .{ .field_type = .object_id, .required = true } },
    });

    const valid = .{ .id = [_]u8{ 0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11 } };
    const result = try DocSchema.validate(testing.allocator, valid);
    try testing.expect(result == null);
}

test "schema - object_id rejects wrong size" {
    const DocSchema = Schema(&.{
        .{ "id", .{ .field_type = .object_id, .required = true } },
    });

    const invalid = .{ .id = @as([]const u8, "short") };
    var verr = (try DocSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("objectId must be 12 bytes", verr.errors[0].message);
}

test "schema - decimal128 accepts [16]u8" {
    const PriceSchema = Schema(&.{
        .{ "price", .{ .field_type = .decimal128, .required = true } },
    });

    const valid = .{ .price = [_]u8{0} ** 16 };
    const result = try PriceSchema.validate(testing.allocator, valid);
    try testing.expect(result == null);
}

test "schema - decimal128 rejects wrong type" {
    const PriceSchema = Schema(&.{
        .{ "price", .{ .field_type = .decimal128 } },
    });

    const invalid = .{ .price = @as(f64, 19.99) };
    var verr = (try PriceSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("expected type 'decimal128' ([16]u8)", verr.errors[0].message);
}

test "schema - uuid accepts [16]u8" {
    const SessionSchema = Schema(&.{
        .{ "uid", .{ .field_type = .uuid, .required = true } },
    });

    const valid = .{ .uid = [_]u8{0} ** 16 };
    const result = try SessionSchema.validate(testing.allocator, valid);
    try testing.expect(result == null);
}

test "schema - uuid rejects wrong size" {
    const SessionSchema = Schema(&.{
        .{ "uid", .{ .field_type = .uuid } },
    });

    const invalid = .{ .uid = [_]u8{0} ** 8 };
    var verr = (try SessionSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("expected type 'uuid' ([16]u8)", verr.errors[0].message);
}

test "schema - timestamp accepts u64" {
    const LogSchema = Schema(&.{
        .{ "ts", .{ .field_type = .timestamp, .required = true } },
    });

    const valid = .{ .ts = @as(u64, 1710000000) };
    const result = try LogSchema.validate(testing.allocator, valid);
    try testing.expect(result == null);
}

test "schema - timestamp rejects non-integer" {
    const LogSchema = Schema(&.{
        .{ "ts", .{ .field_type = .timestamp } },
    });

    const invalid = .{ .ts = @as(f64, 1710000000.0) };
    var verr = (try LogSchema.validate(testing.allocator, invalid)).?;
    defer verr.deinit();
    try testing.expectEqualStrings("expected type 'timestamp' (u64)", verr.errors[0].message);
}

test "schema - comprehensive BSON-aligned schema" {
    const FullSchema = Schema(&.{
        .{ "name", .{ .field_type = .string, .required = true, .min_length = 1 } },
        .{ "age", .{ .field_type = .int32, .min = 0, .max = 150 } },
        .{ "score", .{ .field_type = .double, .min = 0, .max = 100 } },
        .{ "active", .{ .field_type = .boolean } },
        .{ "created", .{ .field_type = .date } },
        .{ "id", .{ .field_type = .object_id } },
        .{ "balance", .{ .field_type = .decimal128 } },
        .{ "uid", .{ .field_type = .uuid } },
        .{ "version", .{ .field_type = .timestamp } },
    });

    const doc = .{
        .name = "Alice",
        .age = @as(i32, 30),
        .score = @as(f64, 95.5),
        .active = true,
        .created = @as(i64, 1710000000000),
        .id = [_]u8{ 0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11 },
        .balance = [_]u8{0} ** 16,
        .uid = [_]u8{0} ** 16,
        .version = @as(u64, 1),
    };

    const result = try FullSchema.validate(testing.allocator, doc);
    try testing.expect(result == null);
}
