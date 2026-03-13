const std = @import("std");
const client = @import("shinydb_zig_client");
const Schema = client.Schema;
const SchemaFieldType = client.SchemaFieldType;
const FieldRule = client.FieldRule;

const testing = std.testing;

const UserSchema = Schema(&.{
    .{ "name", .{ .field_type = .string, .required = true, .min_length = 1, .max_length = 100 } },
    .{ "email", .{ .field_type = .string, .required = true } },
    .{ "age", .{ .field_type = .int, .min = 0, .max = 150 } },
    .{ "role", .{ .field_type = .string, .enum_values = &.{ "admin", "user", "guest" } } },
    .{ "active", .{ .field_type = .boolean } },
});

// Helper to check if a specific field+message error exists
fn hasError(errors: []const client.FieldError, field: []const u8, message: []const u8) bool {
    for (errors) |err| {
        if (std.mem.eql(u8, err.field, field) and std.mem.eql(u8, err.message, message)) return true;
    }
    return false;
}

fn hasFieldError(errors: []const client.FieldError, field: []const u8) bool {
    for (errors) |err| {
        if (std.mem.eql(u8, err.field, field)) return true;
    }
    return false;
}

// ── Valid Struct ──

test "valid struct passes all checks" {
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

test "minimal valid struct (only required fields)" {
    const user = .{
        .name = "Alice",
        .email = "alice@example.com",
    };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

// ── Required ──

test "missing required field 'name'" {
    const user = .{
        .email = "alice@example.com",
    };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasFieldError(verr.errors, "name"));
}

test "missing required field 'email'" {
    const user = .{
        .name = "Alice",
    };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasFieldError(verr.errors, "email"));
}

test "required optional field with null fails" {
    const S = Schema(&.{
        .{ "name", .{ .field_type = .string, .required = true } },
    });
    const user = .{ .name = @as(?[]const u8, null) };
    var verr = (try S.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "name", "is required"));
}

// ── String Length ──

test "string too short" {
    const user = .{ .name = "", .email = "a@b.com" };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "name", "too short"));
}

test "string too long" {
    const long = "A" ** 101;
    const user = .{ .name = @as([]const u8, long), .email = "a@b.com" };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "name", "too long"));
}

test "string at exact max length passes" {
    const exact = "A" ** 100;
    const user = .{ .name = @as([]const u8, exact), .email = "a@b.com" };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "string at exact min length passes" {
    const user = .{ .name = "X", .email = "a@b.com" };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

// ── Int Min/Max ──

test "int below minimum" {
    const user = .{ .name = "Bob", .email = "b@b.com", .age = @as(i32, -5) };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "age", "below minimum"));
}

test "int above maximum" {
    const user = .{ .name = "Bob", .email = "b@b.com", .age = @as(i32, 200) };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "age", "above maximum"));
}

test "int at exact min boundary passes" {
    const user = .{ .name = "Bob", .email = "b@b.com", .age = @as(i32, 0) };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "int at exact max boundary passes" {
    const user = .{ .name = "Bob", .email = "b@b.com", .age = @as(i32, 150) };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

// ── Float ──

test "float within range passes" {
    const S = Schema(&.{
        .{ "price", .{ .field_type = .float, .required = true, .min = 0, .max = 99999 } },
    });
    const doc = .{ .price = @as(f64, 29.99) };
    const result = try S.validate(testing.allocator, doc);
    try testing.expect(result == null);
}

test "float below minimum" {
    const S = Schema(&.{
        .{ "price", .{ .field_type = .float, .min = 0 } },
    });
    const doc = .{ .price = @as(f64, -1.0) };
    var verr = (try S.validate(testing.allocator, doc)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "price", "below minimum"));
}

test "float above maximum" {
    const S = Schema(&.{
        .{ "price", .{ .field_type = .float, .max = 100 } },
    });
    const doc = .{ .price = @as(f64, 200.0) };
    var verr = (try S.validate(testing.allocator, doc)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "price", "above maximum"));
}

// ── Boolean ──

test "boolean true passes" {
    const user = .{ .name = "A", .email = "a@b.com", .active = true };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "boolean false passes" {
    const user = .{ .name = "A", .email = "a@b.com", .active = false };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

// ── Enum ──

test "valid enum value passes" {
    const user = .{ .name = "A", .email = "a@b.com", .role = "admin" };
    const result = try UserSchema.validate(testing.allocator, user);
    try testing.expect(result == null);
}

test "invalid enum value fails" {
    const user = .{ .name = "A", .email = "a@b.com", .role = "superadmin" };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "role", "not in allowed values"));
}

test "all valid enum values pass" {
    for ([_][]const u8{ "admin", "user", "guest" }) |role| {
        const user = .{ .name = "A", .email = "a@b.com", .role = role };
        const result = try UserSchema.validate(testing.allocator, user);
        try testing.expect(result == null);
    }
}

// ── Optional Fields ──

test "optional null passes when not required" {
    const S = Schema(&.{
        .{ "bio", .{ .field_type = .string, .required = false } },
    });
    const doc = .{ .bio = @as(?[]const u8, null) };
    const result = try S.validate(testing.allocator, doc);
    try testing.expect(result == null);
}

test "optional with value validates normally" {
    const S = Schema(&.{
        .{ "bio", .{ .field_type = .string, .required = false, .min_length = 5 } },
    });
    const doc = .{ .bio = @as(?[]const u8, "hi") };
    var verr = (try S.validate(testing.allocator, doc)).?;
    defer verr.deinit();
    try testing.expect(hasError(verr.errors, "bio", "too short"));
}

// ── Multiple Errors ──

test "multiple errors collected" {
    const user = .{
        .name = "",
        .email = "a@b.com",
        .age = @as(i32, -1),
        .role = "superadmin",
    };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    try testing.expect(verr.errors.len >= 3);
}

test "multiple required fields missing" {
    const S = Schema(&.{
        .{ "a", .{ .field_type = .string, .required = true } },
        .{ "b", .{ .field_type = .string, .required = true } },
        .{ "c", .{ .field_type = .int, .required = true } },
    });
    const doc = .{ .x = "unrelated" };
    var verr = (try S.validate(testing.allocator, doc)).?;
    defer verr.deinit();
    try testing.expect(verr.errors.len == 3);
}

// ── Format Error ──

test "format error message" {
    const user = .{ .name = "", .email = "a@b.com" };
    var verr = (try UserSchema.validate(testing.allocator, user)).?;
    defer verr.deinit();
    const msg = try verr.format(testing.allocator);
    defer testing.allocator.free(msg);
    try testing.expect(std.mem.startsWith(u8, msg, "Validation failed: "));
    try testing.expect(std.mem.indexOf(u8, msg, "name: too short") != null);
}

// ── validateAndEncode ──

test "validateAndEncode valid struct returns BSON" {
    const user = .{
        .name = @as([]const u8, "Alice"),
        .email = @as([]const u8, "alice@example.com"),
        .age = @as(i32, 30),
        .role = @as([]const u8, "user"),
        .active = true,
    };
    const encoded = try UserSchema.validateAndEncode(testing.allocator, user);
    defer testing.allocator.free(encoded);
    try testing.expect(encoded.len > 4);
    const doc_len = std.mem.readInt(i32, encoded[0..4], .little);
    try testing.expect(doc_len == @as(i32, @intCast(encoded.len)));
}

test "validateAndEncode invalid struct returns error" {
    const user = .{ .email = @as([]const u8, "alice@example.com") };
    const result = UserSchema.validateAndEncode(testing.allocator, user);
    try testing.expectError(error.ValidationFailed, result);
}
