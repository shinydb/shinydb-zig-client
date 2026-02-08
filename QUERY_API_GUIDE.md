# shinydb Query API Guide

A comprehensive guide to using the shinydb Query Builder API for Zig applications.

## Table of Contents

- [Getting Started](#getting-started)
- [Basic Operations](#basic-operations)
- [CRUD Operations](#crud-operations)
- [Querying Documents](#querying-documents)
- [Aggregations](#aggregations)
- [Advanced Patterns](#advanced-patterns)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [API Reference](#api-reference)

---

## Getting Started

### Installation

Add `shinydb-zig-client` to your `build.zig.zon`:

```zig
.dependencies = .{
    .shinydb_zig_client = .{
        .url = "https://github.com/yourusername/shinydb-zig-client/archive/refs/tags/v0.1.0.tar.gz",
        .hash = "...",
    },
}
```

### Basic Setup

```zig
const std = @import("std");
const shinydb = @import("shinydb_zig_client");
const ShinyDbClient = shinydb.ShinyDbClient;
const Query = shinydb.Query;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Setup I/O
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Create and connect client
    var client = try ShinyDbClient.init(allocator, io);
    defer client.deinit();

    try client.connect("127.0.0.1", 23469);
    defer client.disconnect();

    // Use Query API here...
}
```

---

## Basic Operations

### The Query Pattern

All operations follow a consistent fluent pattern:

```zig
Query.init(client)
    .space("space_name")
    .store("store_name")
    .operation(...)
    .run();
```

**Key Points:**

- Every query starts with `Query.init(client)`
- Chain `.space()` and `.store()` to specify namespace
- Add operation methods (`.create()`, `.where()`, etc.)
- Call `.run()` to execute
- Call `.deinit()` to cleanup

---

## CRUD Operations

### Create Document

```zig
const User = struct {
    id: u64,
    name: []const u8,
    email: []const u8,
    age: u32,
};

const user = User{
    .id = 1,
    .name = "Alice",
    .email = "alice@example.com",
    .age = 28,
};

var query = Query.init(client);
defer query.deinit();

_ = try query.space("myapp")
    .store("users")
    .create(user);

var response = try query.run();
defer response.deinit();

// Get the document key from response
if (response.data) |data| {
    std.debug.print("Created: {s}\n", .{data});
}
```

### Read Document by ID

```zig
var query = Query.init(client);
defer query.deinit();

_ = query.space("myapp")
    .store("users")
    .readById(12345678901234567890);

var response = try query.run();
defer response.deinit();

if (response.data) |data| {
    std.debug.print("User: {s}\n", .{data});
}
```

### Update Document

```zig
const updated_user = User{
    .id = 1,
    .name = "Alice Smith",
    .email = "alice.smith@example.com",
    .age = 29,
};

var query = Query.init(client);
defer query.deinit();

_ = try query.space("myapp")
    .store("users")
    .readById(doc_key)
    .update(updated_user);

var response = try query.run();
defer response.deinit();
```

### Delete Document

```zig
var query = Query.init(client);
defer query.deinit();

_ = query.space("myapp")
    .store("users")
    .readById(doc_key)
    .delete();

try query.run();
```

---

## Querying Documents

### Simple Query with Filter

```zig
var query = Query.init(client);
defer query.deinit();

_ = query.space("myapp")
    .store("users")
    .where("age", .gt, .{ .int = 21 })
    .limit(10);

var response = try query.run();
defer response.deinit();

if (response.data) |data| {
    std.debug.print("Results: {s}\n", .{data});
}
```

### Filter Operators

| Operator   | Enum Value  | Description      | Example                                              |
| ---------- | ----------- | ---------------- | ---------------------------------------------------- |
| `=`        | `.eq`       | Equal            | `.where("status", .eq, .{.string = "active"})`       |
| `!=`       | `.ne`       | Not equal        | `.where("status", .ne, .{.string = "deleted"})`      |
| `>`        | `.gt`       | Greater than     | `.where("age", .gt, .{.int = 21})`                   |
| `>=`       | `.gte`      | Greater or equal | `.where("age", .gte, .{.int = 18})`                  |
| `<`        | `.lt`       | Less than        | `.where("price", .lt, .{.float = 100.0})`            |
| `<=`       | `.lte`      | Less or equal    | `.where("price", .lte, .{.float = 50.0})`            |
| `~`        | `.regex`    | Regex match      | `.where("name", .regex, .{.string = "^John"})`       |
| `in`       | `.in`       | Value in list    | `.where("status", .in, .{.array = &statuses})`       |
| `contains` | `.contains` | Contains value   | `.where("tags", .contains, .{.string = "featured"})` |
| `exists`   | `.exists`   | Field exists     | `.where("email", .exists, .{.bool = true})`          |

### Value Types

```zig
const Value = union(enum) {
    string: []const u8,
    int: i64,
    float: f64,
    bool: bool,
    null_value,
};

// Examples:
.{ .string = "hello" }
.{ .int = 42 }
.{ .float = 3.14 }
.{ .bool = true }
.{ .null_value = {} }
```

### Multiple Filters

Use chaining for AND conditions:

```zig
_ = query.space("myapp")
    .store("orders")
    .where("status", .eq, .{.string = "completed"})
    .where("total", .gt, .{.float = 100.0})
    .where("user_id", .lt, .{.int = 10000});
```

### Sorting Results

```zig
// Ascending order
_ = query.space("myapp")
    .store("users")
    .orderBy("name", .asc)
    .limit(50);

// Descending order
_ = query.space("myapp")
    .store("orders")
    .orderBy("created_at", .desc)
    .limit(20);
```

### Pagination

```zig
// Page 1: First 10 results
_ = query.space("myapp")
    .store("users")
    .orderBy("id", .asc)
    .limit(10);

// Page 2: Skip 10, take 10
_ = query.space("myapp")
    .store("users")
    .orderBy("id", .asc)
    .skip(10)
    .limit(10);

// Page N: Skip (N-1)*pageSize, take pageSize
const page = 3;
const page_size = 10;
_ = query.space("myapp")
    .store("users")
    .orderBy("id", .asc)
    .skip((page - 1) * page_size)
    .limit(page_size);
```

### Complex Query Example

```zig
var query = Query.init(client);
defer query.deinit();

_ = query.space("ecommerce")
    .store("orders")
    .where("status", .eq, .{.string = "completed"})
    .where("total", .gt, .{.float = 50.0})
    .where("total", .lt, .{.float = 500.0})
    .where("user_id", .ne, .{.int = 0})
    .orderBy("created_at", .desc)
    .skip(0)
    .limit(100);

var response = try query.run();
defer response.deinit();
```

---

## Aggregations

### Count Documents

```zig
var query = Query.init(client);
defer query.deinit();

_ = query.space("myapp")
    .store("orders")
    .count("total_orders");

var response = try query.run();
defer response.deinit();

if (response.data) |data| {
    std.debug.print("Result: {s}\n", .{data});
}
```

### Sum Aggregation

```zig
_ = query.space("myapp")
    .store("orders")
    .where("status", .eq, .{.string = "completed"})
    .sum("total_revenue", "amount");
```

### Multiple Aggregations

```zig
_ = query.space("myapp")
    .store("orders")
    .count("order_count")
    .sum("total_revenue", "amount")
    .avg("avg_order_value", "amount")
    .min("min_order", "amount")
    .max("max_order", "amount");
```

### Group By Single Field

```zig
_ = query.space("myapp")
    .store("orders")
    .groupBy("status")
    .count("count")
    .sum("revenue", "total");
```

### Group By Multiple Fields

```zig
_ = query.space("analytics")
    .store("sales")
    .groupBy("region")
    .groupBy("year")
    .count("order_count")
    .sum("revenue", "amount");
```

### Filter + Group + Aggregate

```zig
var query = Query.init(client);
defer query.deinit();

_ = query.space("myapp")
    .store("orders")
    .where("created_at", .gte, .{.int = start_timestamp})
    .where("created_at", .lt, .{.int = end_timestamp})
    .groupBy("customer_id")
    .count("order_count")
    .sum("total_spent", "amount")
    .avg("avg_order_value", "amount");

var response = try query.run();
defer response.deinit();
```

---

## Advanced Patterns

### Range Queries

```zig
// Date range
const start_date = 1640995200; // 2022-01-01 00:00:00
const end_date = 1672531199;   // 2022-12-31 23:59:59

_ = query.space("analytics")
    .store("events")
    .where("timestamp", .gte, .{.int = start_date})
    .where("timestamp", .lte, .{.int = end_date})
    .orderBy("timestamp", .desc);

// Numeric range
_ = query.space("ecommerce")
    .store("products")
    .where("price", .gte, .{.float = 10.0})
    .where("price", .lte, .{.float = 100.0})
    .orderBy("price", .asc);
```

### Scan Operation (Range Reads)

```zig
var query = Query.init(client);
defer query.deinit();

const start_key: ?u128 = null; // Start from beginning
const record_count: u32 = 100;  // Fetch 100 records

_ = query.space("myapp")
    .store("users")
    .scan(record_count, start_key);

var response = try query.run();
defer response.deinit();
```

### Conditional Updates

```zig
// Only update documents matching the filter
var query = Query.init(client);
defer query.deinit();

const updated_data = Order{
    .status = "cancelled",
    .updated_at = std.time.timestamp(),
};

_ = try query.space("myapp")
    .store("orders")
    .where("status", .eq, .{.string = "pending"})
    .where("created_at", .lt, .{.int = cutoff_time})
    .update(updated_data);

try query.run();
```

### Batch Operations (Future Enhancement)

```zig
// Concept for future batch inserts
const users = [_]User{
    .{.id = 1, .name = "Alice", .email = "alice@example.com", .age = 28},
    .{.id = 2, .name = "Bob", .email = "bob@example.com", .age = 32},
    .{.id = 3, .name = "Carol", .email = "carol@example.com", .age = 25},
};

_ = try query.space("myapp")
    .store("users")
    .createBatch(&users);
```

---

## Best Practices

### 1. Always Cleanup Resources

```zig
// ✅ Good - using defer
var query = Query.init(client);
defer query.deinit();

var response = try query.run();
defer response.deinit();

// ❌ Bad - forgetting defer
var query = Query.init(client);
var response = try query.run();
// Memory leak!
```

### 2. Reuse Query Objects

```zig
// ✅ Good - single query object for related operations
var query = Query.init(client);
defer query.deinit();

_ = query.space("myapp").store("users");

// Use for multiple operations
var result1 = try query.where("age", .gt, .{.int = 21}).run();
defer result1.deinit();

// ❌ Bad - creating multiple query objects unnecessarily
var query1 = Query.init(client);
defer query1.deinit();
var result1 = try query1.space("myapp").store("users")
    .where("age", .gt, .{.int = 21}).run();

var query2 = Query.init(client);
defer query2.deinit();
var result2 = try query2.space("myapp").store("users")
    .where("status", .eq, .{.string = "active"}).run();
```

### 3. Use Type-Safe Structs

```zig
// ✅ Good - type-safe struct
const User = struct {
    id: u64,
    name: []const u8,
    email: []const u8,
    age: u32,
};

const user = User{...};
_ = try query.create(user);

// ❌ Bad - raw BSON/JSON strings
const user_json = "{\"name\":\"Alice\",\"email\":\"alice@example.com\"}";
```

### 4. Handle Errors Appropriately

```zig
// ✅ Good - specific error handling
var response = query.run() catch |err| switch (err) {
    error.ConnectionRefused => {
        std.log.err("Database unavailable", .{});
        return err;
    },
    error.DocumentNotFound => {
        std.log.warn("Document not found, using defaults", .{});
        return default_value;
    },
    else => return err,
};
```

### 5. Limit Result Sets

```zig
// ✅ Good - always use limit for queries
_ = query.space("myapp")
    .store("users")
    .where("status", .eq, .{.string = "active"})
    .limit(1000);

// ⚠️  Warning - unbounded query may return millions of documents
_ = query.space("myapp")
    .store("users")
    .where("status", .eq, .{.string = "active"});
```

### 6. Use Indexes for Performance

```zig
// Create index first (one-time setup)
try client.create(shinydb.Index{
    .id = 0,
    .store_id = 0,
    .ns = "myapp.users.email_idx",
    .field = "email",
    .field_type = .String,
    .unique = true,
    .description = "Email lookup index",
    .created_at = 0,
});

// Then queries on email will be fast
_ = query.space("myapp")
    .store("users")
    .where("email", .eq, .{.string = "user@example.com"})
    .limit(1);
```

---

## Error Handling

### Common Errors

```zig
// Connection errors
error.ConnectionRefused
error.ConnectionLost
error.Timeout

// Query errors
error.DocumentNotFound
error.InvalidQuery
error.InvalidResponse

// Data errors
error.InvalidDocument
error.SerializationFailed
error.DeserializationFailed

// Permission errors
error.PermissionDenied
error.Unauthorized
```

### Error Handling Pattern

```zig
var query = Query.init(client);
defer query.deinit();

_ = query.space("myapp")
    .store("users")
    .where("id", .eq, .{.int = user_id})
    .limit(1);

var response = query.run() catch |err| {
    std.log.err("Query failed: {}", .{err});

    // Handle specific errors
    if (err == error.DocumentNotFound) {
        std.log.info("User {d} not found", .{user_id});
        return null;
    }

    if (err == error.ConnectionRefused) {
        std.log.err("Database unavailable", .{});
        return err;
    }

    return err;
};
defer response.deinit();

// Process response
if (response.data) |data| {
    // Parse and use data
    const user = try parseUser(data);
    return user;
}

return null;
```

---

## API Reference

### Query Methods

#### Namespace Building

```zig
pub fn space(self: *Query, name: []const u8) *Query
pub fn store(self: *Query, name: []const u8) *Query
pub fn index(self: *Query, name: []const u8) *Query
```

#### CRUD Operations

```zig
pub fn create(self: *Query, value: anytype) !*Query
pub fn readById(self: *Query, id: u128) *Query
pub fn update(self: *Query, value: anytype) !*Query
pub fn delete(self: *Query) *Query
```

#### Filtering

```zig
pub fn where(self: *Query, field: []const u8, op: FilterOp, value: Value) *Query
```

#### Modifiers

```zig
pub fn orderBy(self: *Query, field: []const u8, direction: OrderDir) *Query
pub fn limit(self: *Query, n: u32) *Query
pub fn skip(self: *Query, n: u32) *Query
pub fn scan(self: *Query, max_records: u32, start_key: ?u128) *Query
```

#### Aggregations

```zig
pub fn groupBy(self: *Query, field: []const u8) *Query
pub fn count(self: *Query, name: []const u8) *Query
pub fn sum(self: *Query, name: []const u8, field: []const u8) *Query
pub fn avg(self: *Query, name: []const u8, field: []const u8) *Query
pub fn min(self: *Query, name: []const u8, field: []const u8) *Query
pub fn max(self: *Query, name: []const u8, field: []const u8) *Query
```

#### Execution & Cleanup

```zig
pub fn run(self: *Query) !QueryResponse
pub fn deinit(self: *Query) void
```

### QueryResponse

```zig
pub const QueryResponse = struct {
    success: bool,
    data: ?[]const u8,
    count: usize,
    allocator: ?Allocator,

    pub fn deinit(self: *QueryResponse) void
};
```

### FilterOp Enum

```zig
pub const FilterOp = enum {
    eq,          // Equal
    ne,          // Not equal
    gt,          // Greater than
    gte,         // Greater than or equal
    lt,          // Less than
    lte,         // Less than or equal
    regex,       // Regex match
    in,          // Value in list
    contains,    // Contains
    exists,      // Field exists
};
```

### OrderDir Enum

```zig
pub const OrderDir = enum {
    asc,   // Ascending
    desc,  // Descending
};
```

---

## Complete Examples

### User Management System

```zig
const User = struct {
    id: u64,
    username: []const u8,
    email: []const u8,
    role: []const u8,
    created_at: i64,
};

// Create user
pub fn createUser(client: *ShinyDbClient, user: User) !void {
    var query = Query.init(client);
    defer query.deinit();

    _ = try query.space("auth")
        .store("users")
        .create(user);

    var response = try query.run();
    defer response.deinit();
}

// Find user by email
pub fn findUserByEmail(client: *ShinyDbClient, email: []const u8) !?[]const u8 {
    var query = Query.init(client);
    defer query.deinit();

    _ = query.space("auth")
        .store("users")
        .where("email", .eq, .{.string = email})
        .limit(1);

    var response = query.run() catch |err| {
        if (err == error.DocumentNotFound) return null;
        return err;
    };
    defer response.deinit();

    return if (response.data) |data|
        try client.allocator.dupe(u8, data)
    else
        null;
}

// List active users
pub fn listActiveUsers(client: *ShinyDbClient, page: u32, page_size: u32) ![]const u8 {
    var query = Query.init(client);
    defer query.deinit();

    _ = query.space("auth")
        .store("users")
        .where("role", .ne, .{.string = "deleted"})
        .orderBy("created_at", .desc)
        .skip((page - 1) * page_size)
        .limit(page_size);

    var response = try query.run();
    defer response.deinit();

    return if (response.data) |data|
        try client.allocator.dupe(u8, data)
    else
        &[_]u8{};
}

// User statistics
pub fn getUserStats(client: *ShinyDbClient) ![]const u8 {
    var query = Query.init(client);
    defer query.deinit();

    _ = query.space("auth")
        .store("users")
        .groupBy("role")
        .count("user_count");

    var response = try query.run();
    defer response.deinit();

    return if (response.data) |data|
        try client.allocator.dupe(u8, data)
    else
        &[_]u8{};
}
```

### E-Commerce Analytics

```zig
const Order = struct {
    id: u64,
    customer_id: u64,
    total: f64,
    status: []const u8,
    created_at: i64,
};

// Sales by customer
pub fn salesByCustomer(client: *ShinyDbClient, start_date: i64, end_date: i64) ![]const u8 {
    var query = Query.init(client);
    defer query.deinit();

    _ = query.space("ecommerce")
        .store("orders")
        .where("status", .eq, .{.string = "completed"})
        .where("created_at", .gte, .{.int = start_date})
        .where("created_at", .lte, .{.int = end_date})
        .groupBy("customer_id")
        .count("order_count")
        .sum("total_spent", "total")
        .avg("avg_order_value", "total");

    var response = try query.run();
    defer response.deinit();

    return if (response.data) |data|
        try client.allocator.dupe(u8, data)
    else
        &[_]u8{};
}

// Top orders
pub fn getTopOrders(client: *ShinyDbClient, limit: u32) ![]const u8 {
    var query = Query.init(client);
    defer query.deinit();

    _ = query.space("ecommerce")
        .store("orders")
        .where("status", .eq, .{.string = "completed"})
        .where("total", .gt, .{.float = 100.0})
        .orderBy("total", .desc)
        .limit(limit);

    var response = try query.run();
    defer response.deinit();

    return if (response.data) |data|
        try client.allocator.dupe(u8, data)
    else
        &[_]u8{};
}
```

---

## Troubleshooting

### Query Returns No Results

**Check:**

1. Space and store exist: Use `client.list(.Space, null)` and `client.list(.Store, null)`
2. Documents exist: Try query without filters
3. Filter values are correct type (`.int` vs `.float` vs `.string`)
4. Field names match document structure

### Connection Errors

```zig
// Verify connection before queries
client.connect("127.0.0.1", 23469) catch |err| {
    std.log.err("Connection failed: {}", .{err});
    return err;
};

// Check if server is running:
// $ ps aux | grep shinydb
// $ lsof -i :23469
```

### Memory Leaks

```zig
// ✅ Always use defer for cleanup
var query = Query.init(client);
defer query.deinit();

var response = try query.run();
defer response.deinit();

// ✅ Check allocator at program end
std.debug.print("Leaked: {} bytes\n", .{gpa.total_requested_bytes - gpa.total_freed_bytes});
```

---

## Additional Resources

- [CLI Guide](../shinydb-cli/CLI_GUIDE.md) - Interactive shell usage
- [Query Interface Design](../docs/query-interface-design.md) - Architecture and design decisions
- [Protocol Documentation](../proto/README.md) - Wire protocol details
- [YCSB Benchmarks](../shinydb-ycsb/README.md) - Performance benchmarks

---

**Happy Querying! 🚀**
