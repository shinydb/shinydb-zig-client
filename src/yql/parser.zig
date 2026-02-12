const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const Lexer = @import("lexer.zig").Lexer;
const Token = @import("lexer.zig").Token;
const TokenType = @import("lexer.zig").TokenType;

const ast = @import("ast.zig");
const QueryAST = ast.QueryAST;
const FilterExpr = ast.FilterExpr;
const FilterOp = ast.FilterOp;
const LogicOp = ast.LogicOp;
const Value = ast.Value;
const OrderByExpr = ast.OrderByExpr;
const OrderDir = ast.OrderDir;
const AggregationExpr = ast.AggregationExpr;
const AggFunc = ast.AggFunc;
const Mutation = ast.Mutation;

pub const ParseError = error{
    UnexpectedToken,
    ExpectedIdentifier,
    ExpectedOperator,
    ExpectedValue,
    ExpectedLParen,
    ExpectedRParen,
    ExpectedComma,
    ExpectedColon,
    InvalidNumber,
    UnknownOperation,
    OutOfMemory,
};

/// Parser for YQL text queries
pub const Parser = struct {
    allocator: Allocator,
    lexer: Lexer,
    current: Token,

    pub fn init(allocator: Allocator, source: []const u8) Parser {
        var lexer = Lexer.init(source);
        const first_token = lexer.next();
        return .{
            .allocator = allocator,
            .lexer = lexer, // Now lexer has correct pos after next()
            .current = first_token,
        };
    }

    /// Parse the YQL source and return an AST
    pub fn parse(allocator: Allocator, source: []const u8) ParseError!QueryAST {
        var parser = Parser.init(allocator, source);
        return parser.parseQuery();
    }

    /// Parse a complete query
    fn parseQuery(self: *Parser) ParseError!QueryAST {
        var query_ast = QueryAST.init(self.allocator);
        errdefer query_ast.deinit();

        // Parse store reference: space.store or just store
        const store_ref = try self.parseStoreRef();
        query_ast.space = store_ref.space;
        query_ast.store = store_ref.store;

        // Parse chain of operations
        while (self.check(.dot)) {
            self.advance();
            try self.parseOperation(&query_ast);
        }

        return query_ast;
    }

    /// Parse space.store or just store
    fn parseStoreRef(self: *Parser) ParseError!struct { space: ?[]const u8, store: []const u8 } {
        const first = try self.expectIdentifier();

        if (self.check(.dot)) {
            // Could be space.store or store.operation
            // Save state for potential backtrack
            const saved_current = self.current;
            const saved_lexer_pos = self.lexer.pos;
            const saved_lexer_line = self.lexer.line;
            const saved_lexer_col = self.lexer.col;

            self.advance(); // consume dot

            if (self.check(.identifier)) {
                const second = self.current.text;
                // Check if second is an operation
                if (isOperation(second)) {
                    // Restore: first is the store, dot is operation start
                    self.current = saved_current;
                    self.lexer.pos = saved_lexer_pos;
                    self.lexer.line = saved_lexer_line;
                    self.lexer.col = saved_lexer_col;
                    return .{ .space = null, .store = first };
                }

                // second is the store, first is the space
                self.advance(); // consume second identifier
                return .{ .space = first, .store = second };
            } else {
                // Not an identifier after dot, restore
                self.current = saved_current;
                self.lexer.pos = saved_lexer_pos;
                self.lexer.line = saved_lexer_line;
                self.lexer.col = saved_lexer_col;
                return .{ .space = null, .store = first };
            }
        }

        return .{ .space = null, .store = first };
    }

    /// Parse a single operation in the chain
    fn parseOperation(self: *Parser, query_ast: *QueryAST) ParseError!void {
        // Handle keywords that are also operation names (e.g. count)
        if (self.current.type == .kw_count) {
            self.advance();
            try self.parseCountOp(query_ast);
            return;
        }

        const op_name = try self.expectIdentifier();

        if (std.mem.eql(u8, op_name, "filter")) {
            try self.parseFilterOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "pluck")) {
            try self.parsePluckOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "orderBy")) {
            try self.parseOrderByOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "limit")) {
            try self.parseLimitOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "skip")) {
            try self.parseSkipOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "groupBy")) {
            try self.parseGroupByOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "aggregate")) {
            try self.parseAggregateOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "insert")) {
            try self.parseInsertOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "set")) {
            try self.parseSetOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "delete")) {
            try self.parseDeleteOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "count")) {
            try self.parseCountOp(query_ast);
        } else if (std.mem.eql(u8, op_name, "get")) {
            try self.parseGetOp(query_ast);
        } else {
            return ParseError.UnknownOperation;
        }
    }

    // === Operation parsers ===

    fn parseFilterOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        // Parse filter expression(s)
        try self.parseFilterExpr(query_ast);

        try self.expectToken(.rparen);
    }

    fn parseFilterExpr(self: *Parser, query_ast: *QueryAST) ParseError!void {
        // Parse first condition
        const filter = try self.parseCondition();
        query_ast.filters.append(self.allocator, filter) catch return ParseError.OutOfMemory;

        // Check for and/or
        while (self.check(.kw_and) or self.check(.kw_or)) {
            const logic: LogicOp = if (self.check(.kw_and)) .@"and" else .@"or";
            self.advance();

            // Update previous filter's logic
            if (query_ast.filters.items.len > 0) {
                query_ast.filters.items[query_ast.filters.items.len - 1].logic = logic;
            }

            const next_filter = try self.parseCondition();
            query_ast.filters.append(self.allocator, next_filter) catch return ParseError.OutOfMemory;
        }
    }

    fn parseCondition(self: *Parser) ParseError!FilterExpr {
        // field op value
        const field = try self.parseFieldPath();
        const op = try self.parseOperator();
        const value = try self.parseValue();

        return .{
            .field = field,
            .op = op,
            .value = value,
        };
    }

    fn parseFieldPath(self: *Parser) ParseError![]const u8 {
        // Support nested fields: address.city
        const field = try self.expectIdentifier();

        // Check for nested path
        while (self.check(.dot)) {
            const saved = self.current;
            self.advance();

            if (self.check(.identifier)) {
                // Check if next is an operator (end of field path)
                const next = self.current;
                if (next.isOperator() or next.type == .kw_and or next.type == .kw_or) {
                    self.current = saved;
                    break;
                }
                // Continue building path (allocator needed for real impl)
                // For now, just return the first part
                self.current = saved;
                break;
            } else {
                self.current = saved;
                break;
            }
        }

        return field;
    }

    fn parseOperator(self: *Parser) ParseError!FilterOp {
        const token = self.current;

        const op: ?FilterOp = switch (token.type) {
            .eq => .eq,
            .ne => .ne,
            .gt => .gt,
            .gte => .gte,
            .lt => .lt,
            .lte => .lte,
            .tilde => .regex,
            .kw_in => .in,
            .kw_contains => .contains,
            .kw_exists => .exists,
            else => null,
        };

        if (op) |o| {
            self.advance();
            return o;
        }

        return ParseError.ExpectedOperator;
    }

    fn parseValue(self: *Parser) ParseError!Value {
        const token = self.current;

        switch (token.type) {
            .string => {
                self.advance();
                return .{ .string = token.text };
            },
            .number => {
                self.advance();
                // Check if it's a float
                if (std.mem.indexOf(u8, token.text, ".") != null) {
                    const f = std.fmt.parseFloat(f64, token.text) catch return ParseError.InvalidNumber;
                    return .{ .float = f };
                } else {
                    const i = std.fmt.parseInt(i64, token.text, 10) catch return ParseError.InvalidNumber;
                    return .{ .int = i };
                }
            },
            .kw_true => {
                self.advance();
                return .{ .bool = true };
            },
            .kw_false => {
                self.advance();
                return .{ .bool = false };
            },
            .kw_null => {
                self.advance();
                return .null;
            },
            else => return ParseError.ExpectedValue,
        }
    }

    fn parsePluckOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        var projection: ArrayList([]const u8) = .empty;
        errdefer projection.deinit(self.allocator);

        // Parse field list
        const first = try self.expectIdentifier();
        projection.append(self.allocator, first) catch return ParseError.OutOfMemory;

        while (self.check(.comma)) {
            self.advance();
            const field = try self.expectIdentifier();
            projection.append(self.allocator, field) catch return ParseError.OutOfMemory;
        }

        try self.expectToken(.rparen);

        query_ast.projection = projection;
    }

    fn parseOrderByOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        const field = try self.expectIdentifier();
        var direction: OrderDir = .asc;

        if (self.check(.comma)) {
            self.advance();
            if (self.check(.kw_asc)) {
                self.advance();
                direction = .asc;
            } else if (self.check(.kw_desc)) {
                self.advance();
                direction = .desc;
            } else {
                return ParseError.UnexpectedToken;
            }
        }

        try self.expectToken(.rparen);

        query_ast.order_by = .{
            .field = field,
            .direction = direction,
        };
    }

    fn parseLimitOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        const num_token = self.current;
        if (num_token.type != .number) return ParseError.ExpectedValue;
        self.advance();

        const n = std.fmt.parseInt(u32, num_token.text, 10) catch return ParseError.InvalidNumber;

        try self.expectToken(.rparen);

        query_ast.limit_val = n;
    }

    fn parseSkipOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        const num_token = self.current;
        if (num_token.type != .number) return ParseError.ExpectedValue;
        self.advance();

        const n = std.fmt.parseInt(u32, num_token.text, 10) catch return ParseError.InvalidNumber;

        try self.expectToken(.rparen);

        query_ast.skip_val = n;
    }

    fn parseGroupByOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        var group_by: ArrayList([]const u8) = .empty;
        errdefer group_by.deinit(self.allocator);

        const first = try self.expectIdentifier();
        group_by.append(self.allocator, first) catch return ParseError.OutOfMemory;

        while (self.check(.comma)) {
            self.advance();
            const field = try self.expectIdentifier();
            group_by.append(self.allocator, field) catch return ParseError.OutOfMemory;
        }

        try self.expectToken(.rparen);

        query_ast.group_by = group_by;
    }

    fn parseAggregateOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        var aggs: ArrayList(AggregationExpr) = .empty;
        errdefer aggs.deinit(self.allocator);

        // Parse: name: func(field) or name: count
        try self.parseAggSpec(&aggs);

        while (self.check(.comma)) {
            self.advance();
            try self.parseAggSpec(&aggs);
        }

        try self.expectToken(.rparen);

        query_ast.aggregations = aggs;
    }

    fn parseAggSpec(self: *Parser, aggs: *ArrayList(AggregationExpr)) ParseError!void {
        const name = try self.expectIdentifier();
        try self.expectToken(.colon);

        // Parse function: count, sum(field), avg(field), etc.
        const func_token = self.current;
        const func: AggFunc = switch (func_token.type) {
            .kw_count => .count,
            .kw_sum => .sum,
            .kw_avg => .avg,
            .kw_min => .min,
            .kw_max => .max,
            else => return ParseError.UnexpectedToken,
        };
        self.advance();

        var field: ?[]const u8 = null;

        // Check for (field) - except count which doesn't need it
        if (self.check(.lparen)) {
            self.advance();
            if (!self.check(.rparen)) {
                field = try self.expectIdentifier();
            }
            try self.expectToken(.rparen);
        }

        aggs.append(self.allocator, .{
            .name = name,
            .func = func,
            .field = field,
        }) catch return ParseError.OutOfMemory;
    }

    fn parseInsertOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        // Parse JSON object as raw string
        const json = try self.parseJsonRaw();

        try self.expectToken(.rparen);

        query_ast.mutation = .{ .insert = json };
    }

    fn parseSetOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        const json = try self.parseJsonRaw();

        try self.expectToken(.rparen);

        query_ast.mutation = .{ .update = json };
    }

    fn parseDeleteOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);
        try self.expectToken(.rparen);

        query_ast.mutation = .delete;
    }

    fn parseCountOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);
        try self.expectToken(.rparen);

        query_ast.query_type = .count;
    }

    fn parseGetOp(self: *Parser, query_ast: *QueryAST) ParseError!void {
        try self.expectToken(.lparen);

        const key = try self.parseValue();

        try self.expectToken(.rparen);

        // Add filter for primary key
        query_ast.filters.append(self.allocator, .{
            .field = "_key",
            .op = .eq,
            .value = key,
        }) catch return ParseError.OutOfMemory;
        query_ast.limit_val = 1;
    }

    fn parseJsonRaw(self: *Parser) ParseError![]const u8 {
        // For now, just consume everything between braces
        if (!self.check(.lbrace)) return ParseError.UnexpectedToken;

        const start = self.lexer.pos - 1; // include opening brace
        var depth: u32 = 1;

        self.advance(); // consume {

        while (depth > 0 and self.current.type != .eof) {
            if (self.check(.lbrace)) {
                depth += 1;
            } else if (self.check(.rbrace)) {
                depth -= 1;
            }
            if (depth > 0) {
                self.advance();
            }
        }

        const end = self.lexer.pos;
        self.advance(); // consume final }

        return self.lexer.source[start..end];
    }

    // === Helper methods ===

    fn advance(self: *Parser) void {
        self.current = self.lexer.next();
    }

    fn check(self: *const Parser, token_type: TokenType) bool {
        return self.current.type == token_type;
    }

    fn expectToken(self: *Parser, token_type: TokenType) ParseError!void {
        if (!self.check(token_type)) {
            return switch (token_type) {
                .lparen => ParseError.ExpectedLParen,
                .rparen => ParseError.ExpectedRParen,
                .comma => ParseError.ExpectedComma,
                .colon => ParseError.ExpectedColon,
                else => ParseError.UnexpectedToken,
            };
        }
        self.advance();
    }

    fn expectIdentifier(self: *Parser) ParseError![]const u8 {
        if (self.current.type != .identifier) {
            return ParseError.ExpectedIdentifier;
        }
        const text = self.current.text;
        self.advance();
        return text;
    }
};

fn isOperation(name: []const u8) bool {
    const ops = [_][]const u8{
        "filter",  "pluck",     "orderBy", "limit", "skip",
        "groupBy", "aggregate", "insert",  "set",   "delete",
        "count",   "get",       "exists",
    };
    for (ops) |op| {
        if (std.mem.eql(u8, name, op)) return true;
    }
    return false;
}

// ============================================================================
// Unit Tests
// ============================================================================

test "Parser simple store reference" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(?[]const u8, null), query_ast.space);
    try std.testing.expectEqualStrings("orders", query_ast.store.?);
}

test "Parser space.store reference" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "sales.orders");
    defer query_ast.deinit();

    try std.testing.expectEqualStrings("sales", query_ast.space.?);
    try std.testing.expectEqualStrings("orders", query_ast.store.?);
}

test "Parser simple filter" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.filter(status = \"active\")");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(usize, 1), query_ast.filters.items.len);

    const filter = query_ast.filters.items[0];
    try std.testing.expectEqualStrings("status", filter.field);
    try std.testing.expectEqual(FilterOp.eq, filter.op);
    try std.testing.expectEqualStrings("active", filter.value.string);
}

test "Parser filter with number" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.filter(total > 100)");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(usize, 1), query_ast.filters.items.len);

    const filter = query_ast.filters.items[0];
    try std.testing.expectEqualStrings("total", filter.field);
    try std.testing.expectEqual(FilterOp.gt, filter.op);
    try std.testing.expectEqual(@as(i64, 100), filter.value.int);
}

test "Parser filter with and" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.filter(status = \"active\" and total > 100)");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(usize, 2), query_ast.filters.items.len);
    try std.testing.expectEqual(LogicOp.@"and", query_ast.filters.items[0].logic);
}

test "Parser limit" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.limit(10)");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(u32, 10), query_ast.limit_val.?);
}

test "Parser orderBy" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.orderBy(created_at, desc)");
    defer query_ast.deinit();

    try std.testing.expectEqualStrings("created_at", query_ast.order_by.?.field);
    try std.testing.expectEqual(OrderDir.desc, query_ast.order_by.?.direction);
}

test "Parser pluck" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.pluck(id, name, total)");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(usize, 3), query_ast.projection.?.items.len);
    try std.testing.expectEqualStrings("id", query_ast.projection.?.items[0]);
    try std.testing.expectEqualStrings("name", query_ast.projection.?.items[1]);
    try std.testing.expectEqualStrings("total", query_ast.projection.?.items[2]);
}

test "Parser groupBy and aggregate" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.groupBy(category).aggregate(total: count, revenue: sum(amount))");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(usize, 1), query_ast.group_by.?.items.len);
    try std.testing.expectEqualStrings("category", query_ast.group_by.?.items[0]);

    try std.testing.expectEqual(@as(usize, 2), query_ast.aggregations.?.items.len);
    try std.testing.expectEqualStrings("total", query_ast.aggregations.?.items[0].name);
    try std.testing.expectEqual(AggFunc.count, query_ast.aggregations.?.items[0].func);
    try std.testing.expectEqualStrings("revenue", query_ast.aggregations.?.items[1].name);
    try std.testing.expectEqual(AggFunc.sum, query_ast.aggregations.?.items[1].func);
    try std.testing.expectEqualStrings("amount", query_ast.aggregations.?.items[1].field.?);
}

test "Parser complex query" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "sales.orders.filter(status = \"active\").orderBy(total, desc).limit(10)");
    defer query_ast.deinit();

    try std.testing.expectEqualStrings("sales", query_ast.space.?);
    try std.testing.expectEqualStrings("orders", query_ast.store.?);
    try std.testing.expectEqual(@as(usize, 1), query_ast.filters.items.len);
    try std.testing.expectEqualStrings("total", query_ast.order_by.?.field);
    try std.testing.expectEqual(OrderDir.desc, query_ast.order_by.?.direction);
    try std.testing.expectEqual(@as(u32, 10), query_ast.limit_val.?);
}

test "Parser delete" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "orders.filter(status = \"cancelled\").delete()");
    defer query_ast.deinit();

    try std.testing.expectEqual(@as(usize, 1), query_ast.filters.items.len);
    try std.testing.expect(query_ast.mutation != null);
    try std.testing.expectEqual(Mutation.delete, query_ast.mutation.?);
}

test "Parser space.store.limit" {
    const allocator = std.testing.allocator;
    var query_ast = try Parser.parse(allocator, "sales.orders.limit(10)");
    defer query_ast.deinit();

    try std.testing.expectEqualStrings("sales", query_ast.space.?);
    try std.testing.expectEqualStrings("orders", query_ast.store.?);
    try std.testing.expectEqual(@as(u32, 10), query_ast.limit_val.?);
}
