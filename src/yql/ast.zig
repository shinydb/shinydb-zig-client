const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

/// Value types that can appear in YQL expressions
pub const Value = union(enum) {
    string: []const u8,
    int: i64,
    float: f64,
    bool: bool,
    null,
    array: []const Value,

    /// Format value to ArrayList for JSON serialization
    pub fn formatTo(self: Value, allocator: Allocator, buf: *ArrayList(u8)) !void {
        switch (self) {
            .string => |s| {
                try buf.append(allocator, '"');
                try buf.appendSlice(allocator, s);
                try buf.append(allocator, '"');
            },
            .int => |i| {
                const str = try std.fmt.allocPrint(allocator, "{d}", .{i});
                defer allocator.free(str);
                try buf.appendSlice(allocator, str);
            },
            .float => |f| {
                // Use {d:.1} to ensure at least 1 decimal place, so JSON parser recognizes it as float
                const str = try std.fmt.allocPrint(allocator, "{d:.1}", .{f});
                defer allocator.free(str);
                try buf.appendSlice(allocator, str);
            },
            .bool => |b| {
                try buf.appendSlice(allocator, if (b) "true" else "false");
            },
            .null => try buf.appendSlice(allocator, "null"),
            .array => |arr| {
                try buf.append(allocator, '[');
                for (arr, 0..) |v, i| {
                    if (i > 0) try buf.appendSlice(allocator, ", ");
                    try v.formatTo(allocator, buf);
                }
                try buf.append(allocator, ']');
            },
        }
    }

    /// Format value to string (for debugging)
    pub fn toString(self: Value, allocator: Allocator) ![]u8 {
        var buf: ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        try self.formatTo(allocator, &buf);
        return buf.toOwnedSlice(allocator);
    }
};

/// Filter comparison operators
pub const FilterOp = enum {
    eq, // =
    ne, // !=
    gt, // >
    gte, // >=
    lt, // <
    lte, // <=
    regex, // ~ (regex match)
    in, // in (value in array)
    contains, // contains (string contains substring)
    starts_with, // string starts with prefix
    exists, // field exists

    pub fn toJsonOp(self: FilterOp) []const u8 {
        return switch (self) {
            .eq => "$eq",
            .ne => "$ne",
            .gt => "$gt",
            .gte => "$gte",
            .lt => "$lt",
            .lte => "$lte",
            .regex => "$regex",
            .in => "$in",
            .contains => "$contains",
            .starts_with => "$startsWith",
            .exists => "$exists",
        };
    }

    pub fn fromString(s: []const u8) ?FilterOp {
        if (std.mem.eql(u8, s, "=")) return .eq;
        if (std.mem.eql(u8, s, "!=")) return .ne;
        if (std.mem.eql(u8, s, ">")) return .gt;
        if (std.mem.eql(u8, s, ">=")) return .gte;
        if (std.mem.eql(u8, s, "<")) return .lt;
        if (std.mem.eql(u8, s, "<=")) return .lte;
        if (std.mem.eql(u8, s, "~")) return .regex;
        if (std.mem.eql(u8, s, "in")) return .in;
        if (std.mem.eql(u8, s, "contains")) return .contains;
        if (std.mem.eql(u8, s, "startsWith")) return .starts_with;
        if (std.mem.eql(u8, s, "exists")) return .exists;
        return null;
    }
};

/// Logical operators for combining conditions
pub const LogicOp = enum {
    none,
    @"and",
    @"or",
};

/// A single filter condition
pub const FilterExpr = struct {
    field: []const u8,
    op: FilterOp,
    value: Value,
    logic: LogicOp = .none, // How this connects to next filter
};

/// Sort direction
pub const OrderDir = enum {
    asc,
    desc,

    pub fn toString(self: OrderDir) []const u8 {
        return switch (self) {
            .asc => "asc",
            .desc => "desc",
        };
    }
};

/// Order by expression
pub const OrderByExpr = struct {
    field: []const u8,
    direction: OrderDir,
};

/// Aggregation functions
pub const AggFunc = enum {
    count,
    sum,
    avg,
    min,
    max,

    pub fn toJsonOp(self: AggFunc) []const u8 {
        return switch (self) {
            .count => "$count",
            .sum => "$sum",
            .avg => "$avg",
            .min => "$min",
            .max => "$max",
        };
    }

    pub fn fromString(s: []const u8) ?AggFunc {
        if (std.mem.eql(u8, s, "count")) return .count;
        if (std.mem.eql(u8, s, "sum")) return .sum;
        if (std.mem.eql(u8, s, "avg")) return .avg;
        if (std.mem.eql(u8, s, "min")) return .min;
        if (std.mem.eql(u8, s, "max")) return .max;
        return null;
    }
};

/// Aggregation expression
pub const AggregationExpr = struct {
    name: []const u8, // Output field name
    func: AggFunc, // Aggregation function
    field: ?[]const u8, // Field to aggregate (null for count)
};

/// Mutation types
pub const Mutation = union(enum) {
    insert: []const u8, // JSON document
    update: []const u8, // JSON update object
    delete,
};

/// Query type
pub const QueryType = enum {
    select, // Normal query
    count, // Count only
    exists, // Check existence
    aggregate, // Aggregation query
};

/// The complete Query AST
pub const QueryAST = struct {
    allocator: Allocator,

    // Target
    space: ?[]const u8 = null,
    store: ?[]const u8 = null,

    // Query type
    query_type: QueryType = .select,

    // Direct document ID access (bypasses query engine)
    doc_id: ?u128 = null,

    // Query operations (Zig 0.16: use .empty, pass allocator to methods)
    filters: ArrayList(FilterExpr) = .empty,
    projection: ?ArrayList([]const u8) = null,
    order_by: ?ArrayList(OrderByExpr) = null,
    limit_val: ?u32 = null,
    skip_val: ?u32 = null,
    group_by: ?ArrayList([]const u8) = null,
    aggregations: ?ArrayList(AggregationExpr) = null,

    // Mutation
    mutation: ?Mutation = null,

    pub fn init(allocator: Allocator) QueryAST {
        return .{
            .allocator = allocator,
            .filters = .empty,
        };
    }

    pub fn deinit(self: *QueryAST) void {
        self.filters.deinit(self.allocator);
        if (self.projection) |*p| p.deinit(self.allocator);
        if (self.order_by) |*o| o.deinit(self.allocator);
        if (self.group_by) |*g| g.deinit(self.allocator);
        if (self.aggregations) |*a| a.deinit(self.allocator);
        // Free mutation payload if present
        if (self.mutation) |mut| {
            switch (mut) {
                .insert => |payload| self.allocator.free(payload),
                .update => |payload| self.allocator.free(payload),
                .delete => {},
            }
        }
    }

    /// Add a filter
    pub fn addFilter(self: *QueryAST, filter: FilterExpr) !void {
        try self.filters.append(self.allocator, filter);
    }

    /// Get full namespace (space.store)
    pub fn getNamespace(self: *const QueryAST) ?[]const u8 {
        if (self.space) |sp| {
            if (self.store) |st| {
                _ = sp;
                return st;
            }
        }
        return self.store;
    }

    /// Convert AST to JSON query format for server
    pub fn toJson(self: *const QueryAST, allocator: Allocator) ![]u8 {
        var buf: ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);

        try buf.append(allocator, '{');
        var first = true;

        // Filters (always include, even if empty - server expects this field)
        if (!first) try buf.append(allocator, ',');
        first = false;

        // Check if any filter uses OR logic
        var has_or = false;
        for (self.filters.items) |filter| {
            if (filter.logic == .@"or") {
                has_or = true;
                break;
            }
        }

        if (has_or and self.filters.items.len > 0) {
            // Compound mode: split filters into groups at OR boundaries
            // Each group is AND'd together, groups are OR'd
            // e.g.: A and B or C and D → $or: [{A, B}, {C, D}]
            try buf.appendSlice(allocator, "\"filter\":{\"$or\":[");

            var group_start: usize = 0;
            var group_idx: usize = 0;
            var i: usize = 0;
            while (i <= self.filters.items.len) {
                // Emit a group when we hit an OR boundary or end of list
                const at_end = i == self.filters.items.len;
                const at_or = !at_end and i > 0 and self.filters.items[i - 1].logic == .@"or";

                if ((at_or or at_end) and i > group_start) {
                    if (group_idx > 0) try buf.append(allocator, ',');
                    group_idx += 1;
                    try buf.append(allocator, '{');
                    try serializeFilterGroup(allocator, &buf, self.filters.items[group_start..i]);
                    try buf.append(allocator, '}');
                    group_start = i;
                }
                i += 1;
            }

            try buf.appendSlice(allocator, "]}");
        } else {
            // Simple AND-only mode (backward compatible)
            try buf.appendSlice(allocator, "\"filter\":{");
            if (self.filters.items.len > 0) {
                try serializeFilterGroup(allocator, &buf, self.filters.items);
            }
            try buf.append(allocator, '}');
        }

        // Projection
        if (self.projection) |proj| {
            if (proj.items.len > 0) {
                if (!first) try buf.append(allocator, ',');
                first = false;
                try buf.appendSlice(allocator, "\"projection\":[");
                for (proj.items, 0..) |field, i| {
                    if (i > 0) try buf.append(allocator, ',');
                    try appendFmt(allocator, &buf, "\"{s}\"", .{field});
                }
                try buf.append(allocator, ']');
            }
        }

        // Order by
        if (self.order_by) |ob| {
            if (ob.items.len > 0) {
                if (!first) try buf.append(allocator, ',');
                first = false;
                if (ob.items.len == 1) {
                    // Single field: backward-compatible object format
                    try appendFmt(allocator, &buf, "\"orderBy\":{{\"field\":\"{s}\",\"direction\":\"{s}\"}}", .{ ob.items[0].field, ob.items[0].direction.toString() });
                } else {
                    // Multi-field: array format
                    try buf.appendSlice(allocator, "\"orderBy\":[");
                    for (ob.items, 0..) |spec, i| {
                        if (i > 0) try buf.append(allocator, ',');
                        try appendFmt(allocator, &buf, "{{\"field\":\"{s}\",\"direction\":\"{s}\"}}", .{ spec.field, spec.direction.toString() });
                    }
                    try buf.append(allocator, ']');
                }
            }
        }

        // Limit
        if (self.limit_val) |lim| {
            if (!first) try buf.append(allocator, ',');
            first = false;
            try appendFmt(allocator, &buf, "\"limit\":{d}", .{lim});
        }

        // Skip
        if (self.skip_val) |sk| {
            if (!first) try buf.append(allocator, ',');
            first = false;
            try appendFmt(allocator, &buf, "\"skip\":{d}", .{sk});
        }

        // Group by
        if (self.group_by) |gb| {
            if (gb.items.len > 0) {
                if (!first) try buf.append(allocator, ',');
                first = false;
                try buf.appendSlice(allocator, "\"group_by\":[");
                for (gb.items, 0..) |field, i| {
                    if (i > 0) try buf.append(allocator, ',');
                    try appendFmt(allocator, &buf, "\"{s}\"", .{field});
                }
                try buf.append(allocator, ']');
            }
        }

        // Aggregations
        if (self.aggregations) |aggs| {
            if (aggs.items.len > 0) {
                if (!first) try buf.append(allocator, ',');
                first = false;
                try buf.appendSlice(allocator, "\"aggregate\":{");
                for (aggs.items, 0..) |agg, i| {
                    if (i > 0) try buf.append(allocator, ',');
                    try appendFmt(allocator, &buf, "\"{s}\":{{", .{agg.name});
                    if (agg.field) |f| {
                        try appendFmt(allocator, &buf, "\"{s}\":\"{s}\"", .{ agg.func.toJsonOp(), f });
                    } else {
                        try appendFmt(allocator, &buf, "\"{s}\":true", .{agg.func.toJsonOp()});
                    }
                    try buf.append(allocator, '}');
                }
                try buf.append(allocator, '}');
            }
        }

        // Count only
        if (self.query_type == .count) {
            if (!first) try buf.append(allocator, ',');
            first = false;
            try buf.appendSlice(allocator, "\"count\":true");
        }

        // Mutation (insert/update/delete)
        if (self.mutation) |mut| {
            if (!first) try buf.append(allocator, ',');
            first = false;
            switch (mut) {
                .insert => |payload| {
                    try buf.appendSlice(allocator, "\"mutation\":{\"type\":\"insert\",\"payload\":\"");
                    // Base64 encode the payload for JSON safety
                    const base64_encoder = std.base64.standard.Encoder;
                    const encoded_size = base64_encoder.calcSize(payload.len);
                    const encoded = try allocator.alloc(u8, encoded_size);
                    defer allocator.free(encoded);
                    _ = base64_encoder.encode(encoded, payload);
                    try buf.appendSlice(allocator, encoded);
                    try buf.appendSlice(allocator, "\"}");
                },
                .update => |payload| {
                    try buf.appendSlice(allocator, "\"mutation\":{\"type\":\"update\",\"payload\":\"");
                    // Base64 encode the payload for JSON safety
                    const base64_encoder = std.base64.standard.Encoder;
                    const encoded_size = base64_encoder.calcSize(payload.len);
                    const encoded = try allocator.alloc(u8, encoded_size);
                    defer allocator.free(encoded);
                    _ = base64_encoder.encode(encoded, payload);
                    try buf.appendSlice(allocator, encoded);
                    try buf.appendSlice(allocator, "\"}");
                },
                .delete => {
                    try buf.appendSlice(allocator, "\"mutation\":{\"type\":\"delete\"}");
                },
            }
        }

        try buf.append(allocator, '}');
        return buf.toOwnedSlice(allocator);
    }
};

/// Helper to append formatted string to ArrayList
fn appendFmt(allocator: Allocator, buf: *ArrayList(u8), comptime fmt: []const u8, args: anytype) !void {
    const str = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(str);
    try buf.appendSlice(allocator, str);
}

/// Serialize a slice of filters as AND-joined JSON: "field":{"$op":val},"field2":{"$op":val}
/// Merges multiple operators on the same field into one object.
fn serializeFilterGroup(allocator: Allocator, buf: *ArrayList(u8), filters: []const FilterExpr) !void {
    // Collect unique field names in order
    var field_order: ArrayList([]const u8) = .empty;
    defer field_order.deinit(allocator);
    for (filters) |filter| {
        var found = false;
        for (field_order.items) |existing| {
            if (std.mem.eql(u8, existing, filter.field)) {
                found = true;
                break;
            }
        }
        if (!found) try field_order.append(allocator, filter.field);
    }

    for (field_order.items, 0..) |field_name, fi| {
        if (fi > 0) try buf.append(allocator, ',');
        try appendFmt(allocator, buf, "\"{s}\":{{", .{field_name});
        var op_first = true;
        for (filters) |filter| {
            if (!std.mem.eql(u8, filter.field, field_name)) continue;
            if (!op_first) try buf.append(allocator, ',');
            op_first = false;
            try appendFmt(allocator, buf, "\"{s}\":", .{filter.op.toJsonOp()});
            try filter.value.formatTo(allocator, buf);
        }
        try buf.append(allocator, '}');
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

test "Value format" {
    const allocator = std.testing.allocator;

    const str_val = Value{ .string = "hello" };
    const str_out = try str_val.toString(allocator);
    defer allocator.free(str_out);
    try std.testing.expectEqualStrings("\"hello\"", str_out);

    const int_val = Value{ .int = 42 };
    const int_out = try int_val.toString(allocator);
    defer allocator.free(int_out);
    try std.testing.expectEqualStrings("42", int_out);

    const bool_val = Value{ .bool = true };
    const bool_out = try bool_val.toString(allocator);
    defer allocator.free(bool_out);
    try std.testing.expectEqualStrings("true", bool_out);
}

test "FilterOp toJsonOp" {
    try std.testing.expectEqualStrings("$eq", FilterOp.eq.toJsonOp());
    try std.testing.expectEqualStrings("$gt", FilterOp.gt.toJsonOp());
    try std.testing.expectEqualStrings("$regex", FilterOp.regex.toJsonOp());
}

test "QueryAST init and deinit" {
    const allocator = std.testing.allocator;
    var query_ast = QueryAST.init(allocator);
    defer query_ast.deinit();

    try query_ast.addFilter(.{
        .field = "status",
        .op = .eq,
        .value = .{ .string = "active" },
    });

    try std.testing.expectEqual(@as(usize, 1), query_ast.filters.items.len);
}

test "QueryAST toJson simple filter" {
    const allocator = std.testing.allocator;
    var query_ast = QueryAST.init(allocator);
    defer query_ast.deinit();

    try query_ast.addFilter(.{
        .field = "status",
        .op = .eq,
        .value = .{ .string = "active" },
    });

    const json = try query_ast.toJson(allocator);
    defer allocator.free(json);

    try std.testing.expectEqualStrings("{\"filter\":{\"status\":{\"$eq\":\"active\"}}}", json);
}

test "QueryAST toJson with limit" {
    const allocator = std.testing.allocator;
    var query_ast = QueryAST.init(allocator);
    defer query_ast.deinit();

    query_ast.limit_val = 10;

    const json = try query_ast.toJson(allocator);
    defer allocator.free(json);

    // Always includes filter field (even if empty) as server expects it
    try std.testing.expectEqualStrings("{\"filter\":{},\"limit\":10}", json);
}

test "QueryAST toJson complex" {
    const allocator = std.testing.allocator;
    var query_ast = QueryAST.init(allocator);
    defer query_ast.deinit();

    try query_ast.addFilter(.{
        .field = "status",
        .op = .eq,
        .value = .{ .string = "active" },
    });
    try query_ast.addFilter(.{
        .field = "total",
        .op = .gt,
        .value = .{ .int = 100 },
    });

    query_ast.order_by = .{ .field = "created_at", .direction = .desc };
    query_ast.limit_val = 10;

    const json = try query_ast.toJson(allocator);
    defer allocator.free(json);

    // Verify it contains expected parts
    try std.testing.expect(std.mem.indexOf(u8, json, "\"filter\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":{\"$eq\":\"active\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"total\":{\"$gt\":100}") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"orderBy\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"limit\":10") != null);
}
