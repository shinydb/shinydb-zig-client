//! YQL (shinydb Query Language)
//!
//! A chainable query language for shinydb that supports:
//! - Text form for CLI: `orders.filter(status = "active").limit(10)`
//! - Native form for Zig API: `store.query().filter(...).limit(10)`
//!
//! Both forms compile to the same internal AST.

pub const ast = @import("ast.zig");
pub const lexer = @import("lexer.zig");
pub const parser = @import("parser.zig");
pub const builder = @import("builder.zig");

// Re-export main types
pub const QueryAST = ast.QueryAST;
pub const FilterExpr = ast.FilterExpr;
pub const FilterOp = ast.FilterOp;
pub const Value = ast.Value;
pub const OrderDir = ast.OrderDir;
pub const AggFunc = ast.AggFunc;

pub const Lexer = lexer.Lexer;
pub const Token = lexer.Token;
pub const TokenType = lexer.TokenType;

pub const Parser = parser.Parser;
pub const ParseError = parser.ParseError;

pub const Query = builder.Query;

/// Parse YQL text into an AST
pub fn parse(allocator: @import("std").mem.Allocator, source: []const u8) ParseError!QueryAST {
    return Parser.parse(allocator, source);
}

/// Execute YQL text and return results (requires client)
pub fn execute(
    allocator: @import("std").mem.Allocator,
    client: anytype,
    source: []const u8,
) ![]u8 {
    var query_ast = try parse(allocator, source);
    defer query_ast.deinit();

    const json = try query_ast.toJson(allocator);
    defer allocator.free(json);

    // Get namespace
    const ns = if (query_ast.space) |sp|
        try @import("std").fmt.allocPrint(allocator, "{s}.{s}", .{ sp, query_ast.store.? })
    else
        query_ast.store.?;

    defer if (query_ast.space != null) allocator.free(ns);

    // Execute via client
    return client.queryRaw(ns, json);
}

// ============================================================================
// Tests
// ============================================================================

test {
    @import("std").testing.refAllDecls(@This());
}

test "parse and toJson roundtrip" {
    const std = @import("std");
    const allocator = std.testing.allocator;

    var query_ast = try parse(allocator, "orders.filter(status = \"active\").limit(10)");
    defer query_ast.deinit();

    const json = try query_ast.toJson(allocator);
    defer allocator.free(json);

    // Verify JSON structure
    try std.testing.expect(std.mem.indexOf(u8, json, "\"filter\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":{\"$eq\":\"active\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"limit\":10") != null);
}
