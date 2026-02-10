const std = @import("std");
const testing = std.testing;

const sdb = @import("shinydb_zig_client");
const Query = sdb.Query;
const ast = sdb.ast;
const QueryAST = ast.QueryAST;
const FilterOp = ast.FilterOp;
const Value = ast.Value;
const OrderDir = ast.OrderDir;
const AggFunc = ast.AggFunc;

// ============================================================================
// Helper: create a Query without a real ShinyDbClient
// We bypass Query.init() since it requires *ShinyDbClient (which needs std.Io).
// Builder methods only use self.allocator and self.ast — they never dereference client.
// ============================================================================
fn testQuery() Query {
    return .{
        .client = undefined, // never dereferenced in builder methods
        .allocator = testing.allocator,
        .ast = QueryAST.init(testing.allocator),
    };
}

// ============================================================================
// Namespace Building Tests
// ============================================================================

test "namespace: space only" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks");

    try testing.expectEqualStrings("adventureworks", q.space_name.?);
    try testing.expect(q.store_name == null);
    try testing.expect(q.index_name == null);
    try testing.expectEqualStrings("adventureworks", q.ast.space.?);
}

test "namespace: space + store" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products");

    try testing.expectEqualStrings("adventureworks", q.space_name.?);
    try testing.expectEqualStrings("products", q.store_name.?);
    try testing.expect(q.index_name == null);
    try testing.expectEqualStrings("products", q.ast.store.?);
}

test "namespace: space + store + index" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products").index("product_name_idx");

    try testing.expectEqualStrings("adventureworks", q.space_name.?);
    try testing.expectEqualStrings("products", q.store_name.?);
    try testing.expectEqualStrings("product_name_idx", q.index_name.?);
}

// ============================================================================
// Filter Tests (based on product/employee/vendor test data)
// ============================================================================

test "filter: single where eq string — CategoryName = Bikes" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("productcategories")
        .where("CategoryName", .eq, .{ .string = "Bikes" });

    try testing.expectEqual(@as(usize, 1), q.ast.filters.items.len);
    const f = q.ast.filters.items[0];
    try testing.expectEqualStrings("CategoryName", f.field);
    try testing.expectEqual(FilterOp.eq, f.op);
    try testing.expectEqualStrings("Bikes", f.value.string);
}

test "filter: where with int comparison — ListPrice > 1000" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .where("ListPrice", .gt, .{ .int = 1000 });

    try testing.expectEqual(@as(usize, 1), q.ast.filters.items.len);
    const f = q.ast.filters.items[0];
    try testing.expectEqualStrings("ListPrice", f.field);
    try testing.expectEqual(FilterOp.gt, f.op);
    try testing.expectEqual(@as(i64, 1000), f.value.int);
}

test "filter: where with float — StandardCost <= 50.0" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .where("StandardCost", .lte, .{ .float = 50.0 });

    try testing.expectEqual(@as(usize, 1), q.ast.filters.items.len);
    const f = q.ast.filters.items[0];
    try testing.expectEqual(FilterOp.lte, f.op);
    try testing.expectEqual(@as(f64, 50.0), f.value.float);
}

test "filter: where + and — MakeFlag = 1 AND ListPrice > 100" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .where("MakeFlag", .eq, .{ .int = 1 })
        .@"and"("ListPrice", .gt, .{ .int = 100 });

    try testing.expectEqual(@as(usize, 2), q.ast.filters.items.len);

    // First filter should have logic = .and (connecting to next)
    try testing.expectEqualStrings("MakeFlag", q.ast.filters.items[0].field);
    try testing.expectEqual(ast.LogicOp.@"and", q.ast.filters.items[0].logic);

    // Second filter
    try testing.expectEqualStrings("ListPrice", q.ast.filters.items[1].field);
    try testing.expectEqual(FilterOp.gt, q.ast.filters.items[1].op);
    try testing.expectEqual(ast.LogicOp.none, q.ast.filters.items[1].logic);
}

test "filter: where + or — Territory = Northeast OR Territory = Australia" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("employees")
        .where("Territory", .eq, .{ .string = "Northeast" })
        .@"or"("Territory", .eq, .{ .string = "Australia" });

    try testing.expectEqual(@as(usize, 2), q.ast.filters.items.len);
    try testing.expectEqual(ast.LogicOp.@"or", q.ast.filters.items[0].logic);
    try testing.expectEqualStrings("Australia", q.ast.filters.items[1].value.string);
}

test "filter: multiple conditions — employee complex filter" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("employees")
        .where("Gender", .eq, .{ .string = "M" })
        .@"and"("MaritalStatus", .eq, .{ .string = "S" })
        .@"and"("OrganizationLevel", .gte, .{ .int = 3 });

    try testing.expectEqual(@as(usize, 3), q.ast.filters.items.len);
    try testing.expectEqual(ast.LogicOp.@"and", q.ast.filters.items[0].logic);
    try testing.expectEqual(ast.LogicOp.@"and", q.ast.filters.items[1].logic);
    try testing.expectEqual(ast.LogicOp.none, q.ast.filters.items[2].logic);
}

// ============================================================================
// Query Modifier Tests
// ============================================================================

test "orderBy ascending — ProductName asc" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .orderBy("ProductName", .asc);

    try testing.expect(q.ast.order_by != null);
    try testing.expectEqualStrings("ProductName", q.ast.order_by.?.field);
    try testing.expectEqual(OrderDir.asc, q.ast.order_by.?.direction);
}

test "orderBy descending — ListPrice desc" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .orderBy("ListPrice", .desc);

    try testing.expectEqualStrings("ListPrice", q.ast.order_by.?.field);
    try testing.expectEqual(OrderDir.desc, q.ast.order_by.?.direction);
}

test "limit" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products").limit(10);

    try testing.expectEqual(@as(u32, 10), q.ast.limit_val.?);
}

test "skip" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products").skip(20);

    try testing.expectEqual(@as(u32, 20), q.ast.skip_val.?);
}

test "select/projection — ProductID, ProductName, ListPrice" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .select(&.{ "ProductID", "ProductName", "ListPrice" });

    try testing.expect(q.ast.projection != null);
    try testing.expectEqual(@as(usize, 3), q.ast.projection.?.items.len);
    try testing.expectEqualStrings("ProductID", q.ast.projection.?.items[0]);
    try testing.expectEqualStrings("ProductName", q.ast.projection.?.items[1]);
    try testing.expectEqualStrings("ListPrice", q.ast.projection.?.items[2]);
}

// ============================================================================
// Mutation Tests
// ============================================================================

test "delete sets mutation" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .where("ActiveFlag", .eq, .{ .int = 0 })
        .delete();

    try testing.expect(q.ast.mutation != null);
    try testing.expectEqual(ast.Mutation.delete, q.ast.mutation.?);
}

// ============================================================================
// ReadById / Scan Tests
// ============================================================================

test "readById sets id" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products").readById(42);

    try testing.expectEqual(@as(u128, 42), q.read_by_id_value.?);
}

test "scan with no start key" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products").scan(100, null);

    try testing.expect(q.scan_params != null);
    try testing.expectEqual(@as(u32, 100), q.scan_params.?.count);
    try testing.expect(q.scan_params.?.start_key == null);
}

test "scan with start key" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products").scan(50, 999);

    try testing.expect(q.scan_params != null);
    try testing.expectEqual(@as(u32, 50), q.scan_params.?.count);
    try testing.expectEqual(@as(u128, 999), q.scan_params.?.start_key.?);
}

// ============================================================================
// Aggregation Tests (based on order/product test data)
// ============================================================================

test "groupBy — SubCategoryID" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .groupBy("SubCategoryID");

    try testing.expect(q.ast.group_by != null);
    try testing.expectEqual(@as(usize, 1), q.ast.group_by.?.items.len);
    try testing.expectEqualStrings("SubCategoryID", q.ast.group_by.?.items[0]);
}

test "aggregation: count" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .groupBy("SubCategoryID")
        .count("total_count");

    try testing.expect(q.ast.aggregations != null);
    try testing.expectEqual(@as(usize, 1), q.ast.aggregations.?.items.len);
    try testing.expectEqualStrings("total_count", q.ast.aggregations.?.items[0].name);
    try testing.expectEqual(AggFunc.count, q.ast.aggregations.?.items[0].func);
    try testing.expect(q.ast.aggregations.?.items[0].field == null);
}

test "aggregation: sum + avg" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .sum("revenue", "ListPrice")
        .avg("avg_cost", "StandardCost");

    try testing.expect(q.ast.aggregations != null);
    try testing.expectEqual(@as(usize, 2), q.ast.aggregations.?.items.len);

    try testing.expectEqualStrings("revenue", q.ast.aggregations.?.items[0].name);
    try testing.expectEqual(AggFunc.sum, q.ast.aggregations.?.items[0].func);
    try testing.expectEqualStrings("ListPrice", q.ast.aggregations.?.items[0].field.?);

    try testing.expectEqualStrings("avg_cost", q.ast.aggregations.?.items[1].name);
    try testing.expectEqual(AggFunc.avg, q.ast.aggregations.?.items[1].func);
    try testing.expectEqualStrings("StandardCost", q.ast.aggregations.?.items[1].field.?);
}

test "aggregation: min + max" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .min("cheapest", "ListPrice")
        .max("most_expensive", "ListPrice");

    try testing.expect(q.ast.aggregations != null);
    try testing.expectEqual(@as(usize, 2), q.ast.aggregations.?.items.len);
    try testing.expectEqual(AggFunc.min, q.ast.aggregations.?.items[0].func);
    try testing.expectEqual(AggFunc.max, q.ast.aggregations.?.items[1].func);
}

// ============================================================================
// Full Query Chain + AST JSON Tests
// ============================================================================

test "full query chain — products filtered, ordered, paginated" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .where("MakeFlag", .eq, .{ .int = 1 })
        .@"and"("ListPrice", .gt, .{ .int = 100 })
        .orderBy("ListPrice", .desc)
        .limit(10)
        .skip(0)
        .select(&.{ "ProductID", "ProductName", "ListPrice" });

    // Verify all components are set
    try testing.expectEqualStrings("adventureworks", q.space_name.?);
    try testing.expectEqualStrings("products", q.store_name.?);
    try testing.expectEqual(@as(usize, 2), q.ast.filters.items.len);
    try testing.expectEqualStrings("ListPrice", q.ast.order_by.?.field);
    try testing.expectEqual(OrderDir.desc, q.ast.order_by.?.direction);
    try testing.expectEqual(@as(u32, 10), q.ast.limit_val.?);
    try testing.expectEqual(@as(u32, 0), q.ast.skip_val.?);
    try testing.expectEqual(@as(usize, 3), q.ast.projection.?.items.len);
}

test "AST toJson — product filter query" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .where("MakeFlag", .eq, .{ .int = 1 })
        .orderBy("ListPrice", .desc)
        .limit(10);

    const json = try q.ast.toJson(testing.allocator);
    defer testing.allocator.free(json);

    // Verify JSON contains expected parts
    try testing.expect(std.mem.indexOf(u8, json, "\"filter\":{") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"MakeFlag\":{\"$eq\":1}") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"orderBy\":{") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"ListPrice\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"desc\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"limit\":10") != null);
}

test "AST toJson — aggregation query (orders by employee)" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("orders")
        .groupBy("EmployeeID")
        .count("order_count")
        .sum("total_revenue", "TotalDue");

    const json = try q.ast.toJson(testing.allocator);
    defer testing.allocator.free(json);

    // Verify aggregation JSON
    try testing.expect(std.mem.indexOf(u8, json, "\"group_by\":[\"EmployeeID\"]") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"aggregate\":{") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"order_count\":{\"$count\":true}") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"total_revenue\":{\"$sum\":\"TotalDue\"}") != null);
}

test "AST toJson — vendor filter with active flag" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("vendors")
        .where("ActiveFlag", .eq, .{ .int = 1 })
        .@"and"("CreditRating", .lte, .{ .int = 2 })
        .orderBy("VendorName", .asc)
        .limit(50);

    const json = try q.ast.toJson(testing.allocator);
    defer testing.allocator.free(json);

    try testing.expect(std.mem.indexOf(u8, json, "\"ActiveFlag\":{\"$eq\":1}") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"CreditRating\":{\"$lte\":2}") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"limit\":50") != null);
}

test "AST toJson — empty filter produces empty filter object" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products").limit(5);

    const json = try q.ast.toJson(testing.allocator);
    defer testing.allocator.free(json);

    try testing.expect(std.mem.indexOf(u8, json, "\"filter\":{}") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"limit\":5") != null);
}

test "AST toJson — delete mutation" {
    var q = testQuery();
    defer q.deinit();

    _ = q.space("adventureworks").store("products")
        .where("ActiveFlag", .eq, .{ .int = 0 })
        .delete();

    const json = try q.ast.toJson(testing.allocator);
    defer testing.allocator.free(json);

    try testing.expect(std.mem.indexOf(u8, json, "\"mutation\":{\"type\":\"delete\"}") != null);
}
