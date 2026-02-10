const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const bson = @import("bson");
const proto = @import("proto");
const ShinyDbClient = @import("../shinydb_client.zig").ShinyDbClient;

const ast = @import("ast.zig");
const QueryAST = ast.QueryAST;
const FilterOp = ast.FilterOp;
const Value = ast.Value;
const OrderDir = ast.OrderDir;
const AggFunc = ast.AggFunc;

/// Fluent query builder for shinydb with client integration
/// Usage: Query.init(client).space("myspace").store("mystore").create(doc).run()
pub const Query = struct {
    client: *ShinyDbClient,
    allocator: Allocator,

    // Namespace components
    space_name: ?[]const u8 = null,
    store_name: ?[]const u8 = null,
    index_name: ?[]const u8 = null,

    // Query AST (filters, order, limit, aggregations, mutations)
    ast: QueryAST,

    // Special case for read by ID (not part of AST)
    read_by_id_value: ?u128 = null,

    // Special case for scan operation
    scan_params: ?struct {
        start_key: ?u128,
        count: u32,
    } = null,

    // --- Initialization ---

    pub fn init(client: *ShinyDbClient) Query {
        return .{
            .client = client,
            .allocator = client.allocator,
            .ast = QueryAST.init(client.allocator),
        };
    }

    // --- Namespace Building ---

    pub fn space(self: *Query, name: []const u8) *Query {
        self.space_name = name;
        self.ast.space = name;
        return self;
    }

    pub fn store(self: *Query, name: []const u8) *Query {
        self.store_name = name;
        self.ast.store = name;
        return self;
    }

    pub fn index(self: *Query, name: []const u8) *Query {
        self.index_name = name;
        return self;
    }

    // --- Document CRUD Operations ---

    /// Create a document (encodes to BSON and stores in AST)
    pub fn create(self: *Query, value: anytype) !*Query {
        // Encode value to BSON immediately (we can't preserve type through anyopaque)
        var encoder = try bson.Encoder.initWithCapacity(self.allocator, 655360);
        defer encoder.deinit();

        const encoded = try encoder.encode(value);

        // Store BSON bytes in AST mutation as JSON string (for now)
        // TODO: AST should support storing raw BSON bytes
        self.ast.mutation = .{ .insert = encoded };
        return self;
    }

    /// Read a document by ID
    pub fn readById(self: *Query, id: u128) *Query {
        // Store ID for later execution - we'll handle this in executeOperation
        // For now, store as a special marker in a field
        self.read_by_id_value = id;
        return self;
    }

    /// Update documents (with optional ID) - generic
    pub fn update(self: *Query, value: anytype) !*Query {
        // Encode value to BSON immediately
        var encoder = try bson.Encoder.initWithCapacity(self.allocator, 655360);
        defer encoder.deinit();

        const encoded = try encoder.encode(value);
        self.ast.mutation = .{ .update = encoded };
        return self;
    }

    /// Batch insert multiple documents
    pub fn batchInsert(self: *Query, values: anytype) !*Query {
        // Encode array of values to BSON
        var encoder = try bson.Encoder.initWithCapacity(self.allocator, 655360);
        defer encoder.deinit();

        const encoded = try encoder.encode(values);
        // Store as insert for now, server will handle batch
        self.ast.mutation = .{ .insert = encoded };
        return self;
    }

    /// Range query - get documents in a range
    pub fn range(self: *Query, start_id: u128, end_id: u128, max_count: ?u32) *Query {
        // Store range parameters - we'll implement server-side later
        _ = start_id;
        _ = end_id;
        _ = max_count;
        // TODO: Add range support to AST
        return self;
    }

    /// Delete documents matching current filters
    pub fn delete(self: *Query) *Query {
        self.ast.mutation = .delete;
        return self;
    }

    /// Scan documents with limit and optional start key
    pub fn scan(self: *Query, max_records: u32, start_key: ?u128) *Query {
        self.scan_params = .{
            .start_key = start_key,
            .count = max_records,
        };
        return self;
    }

    // --- Query Filters (Unified SQL-like API) ---

    /// Add first filter condition (WHERE clause)
    pub fn where(self: *Query, field: []const u8, op: FilterOp, value: Value) *Query {
        self.ast.filters.append(self.allocator, .{
            .field = field,
            .op = op,
            .value = value,
            .logic = .none,
        }) catch {};
        return self;
    }

    /// Add AND filter condition
    pub fn @"and"(self: *Query, field: []const u8, op: FilterOp, value: Value) *Query {
        if (self.ast.filters.items.len > 0) {
            self.ast.filters.items[self.ast.filters.items.len - 1].logic = .@"and";
        }
        self.ast.filters.append(self.allocator, .{
            .field = field,
            .op = op,
            .value = value,
            .logic = .none,
        }) catch {};
        return self;
    }

    /// Add OR filter condition
    pub fn @"or"(self: *Query, field: []const u8, op: FilterOp, value: Value) *Query {
        if (self.ast.filters.items.len > 0) {
            self.ast.filters.items[self.ast.filters.items.len - 1].logic = .@"or";
        }
        self.ast.filters.append(self.allocator, .{
            .field = field,
            .op = op,
            .value = value,
            .logic = .none,
        }) catch {};
        return self;
    }

    // --- Query Modifiers ---

    /// Set order by field and direction (.asc or .desc)
    pub fn orderBy(self: *Query, field: []const u8, direction: OrderDir) *Query {
        self.ast.order_by = .{ .field = field, .direction = direction };
        return self;
    }

    pub fn limit(self: *Query, n: u32) *Query {
        self.ast.limit_val = n;
        return self;
    }

    pub fn skip(self: *Query, n: u32) *Query {
        self.ast.skip_val = n;
        return self;
    }

    pub fn select(self: *Query, fields: []const []const u8) *Query {
        var proj = ArrayList([]const u8).empty;
        for (fields) |f| {
            proj.append(self.allocator, f) catch {};
        }
        self.ast.projection = proj;
        return self;
    }

    // --- Aggregations ---

    pub fn groupBy(self: *Query, field: []const u8) *Query {
        if (self.ast.group_by == null) {
            self.ast.group_by = .empty;
        }
        self.ast.group_by.?.append(self.allocator, field) catch {};
        return self;
    }

    pub fn count(self: *Query, name: []const u8) *Query {
        return self.aggregate(name, .count, null);
    }

    pub fn sum(self: *Query, name: []const u8, field: []const u8) *Query {
        return self.aggregate(name, .sum, field);
    }

    pub fn avg(self: *Query, name: []const u8, field: []const u8) *Query {
        return self.aggregate(name, .avg, field);
    }

    pub fn min(self: *Query, name: []const u8, field: []const u8) *Query {
        return self.aggregate(name, .min, field);
    }

    pub fn max(self: *Query, name: []const u8, field: []const u8) *Query {
        return self.aggregate(name, .max, field);
    }

    fn aggregate(self: *Query, name: []const u8, func: AggFunc, field: ?[]const u8) *Query {
        if (self.ast.aggregations == null) {
            self.ast.aggregations = .empty;
        }
        self.ast.aggregations.?.append(self.allocator, .{
            .name = name,
            .func = func,
            .field = field,
        }) catch {};
        return self;
    }

    // --- EXECUTE (UNIFIED) ---

    pub fn run(self: *Query) !QueryResponse {
        // Build namespace
        const ns = try self.buildNamespace();
        defer self.allocator.free(ns);

        // Build and execute operation based on query state
        return try self.executeOperation(ns);
    }

    fn buildNamespace(self: *Query) ![]const u8 {
        if (self.space_name == null) {
            return error.NoSpaceSpecified;
        }

        if (self.index_name) |idx| {
            return try std.fmt.allocPrint(
                self.allocator,
                "{s}.{s}.{s}",
                .{ self.space_name.?, self.store_name.?, idx },
            );
        } else if (self.store_name) |store_val| {
            return try std.fmt.allocPrint(
                self.allocator,
                "{s}.{s}",
                .{ self.space_name.?, store_val },
            );
        } else {
            return try self.allocator.dupe(u8, self.space_name.?);
        }
    }

    fn executeOperation(self: *Query, ns: []const u8) !QueryResponse {
        // Check for scan operation first
        if (self.scan_params) |params| {
            return try self.executeScan(ns, params.start_key, params.count);
        }

        // Check for read by ID
        if (self.read_by_id_value) |id| {
            return try self.executeReadById(ns, id);
        }

        // Execute based on mutation type or filters
        if (self.ast.mutation) |mut| {
            return switch (mut) {
                .insert => |bson_bytes| try self.executeCreate(ns, bson_bytes),
                .update => |bson_bytes| try self.executeUpdate(ns, bson_bytes),
                .delete => try self.executeDelete(ns),
            };
        } else if (self.ast.aggregations != null) {
            return try self.executeAggregate(ns);
        } else if (self.ast.filters.items.len > 0 or
            self.ast.limit_val != null or
            self.ast.skip_val != null or
            self.ast.order_by != null or
            self.ast.projection != null or
            self.store_name != null)
        {
            // Execute query if we have filters, modifiers, or just a store name
            return try self.executeQuery(ns);
        }

        return error.NoOperation;
    }

    fn executeCreate(self: *Query, ns: []const u8, bson_bytes: []const u8) !QueryResponse {
        // Use Insert operation with pre-encoded BSON
        const op = proto.Operation{
            .Insert = .{
                .store_ns = ns,
                .payload = bson_bytes,
                .auto_create = true,
            },
        };

        const packet = try self.client.doOperation(op);
        errdefer proto.Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    proto.Packet.free(self.allocator, packet);
                    return error.OperationFailed;
                }
                // Dupe data before freeing packet (reply.data is a slice into packet memory)
                const duped = if (reply.data) |d| try self.allocator.dupe(u8, d) else null;
                proto.Packet.free(self.allocator, packet);
                break :blk QueryResponse{
                    .success = true,
                    .data = duped,
                    .count = 1,
                    .allocator = self.allocator,
                };
            },
            else => {
                proto.Packet.free(self.allocator, packet);
                return error.InvalidResponse;
            },
        };
    }

    fn executeReadById(self: *Query, ns: []const u8, id: u128) !QueryResponse {
        const op = proto.Operation{
            .Read = .{
                .store_ns = ns,
                .id = id,
            },
        };

        const packet = try self.client.doOperation(op);
        errdefer proto.Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    proto.Packet.free(self.allocator, packet);
                    return error.DocumentNotFound;
                }
                // Dupe data before freeing packet (reply.data is a slice into packet memory)
                const duped = if (reply.data) |d| try self.allocator.dupe(u8, d) else null;
                proto.Packet.free(self.allocator, packet);
                break :blk QueryResponse{
                    .success = true,
                    .data = duped,
                    .count = 1,
                    .allocator = self.allocator,
                };
            },
            else => {
                proto.Packet.free(self.allocator, packet);
                return error.InvalidResponse;
            },
        };
    }

    fn executeUpdate(self: *Query, ns: []const u8, bson_bytes: []const u8) !QueryResponse {
        // TODO: Support update by ID
        const op = proto.Operation{
            .Update = .{
                .store_ns = ns,
                .id = 0, // TODO: Get ID from somewhere
                .payload = bson_bytes,
            },
        };

        const packet = try self.client.doOperation(op);
        defer proto.Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    return error.UpdateFailed;
                }
                break :blk QueryResponse{
                    .success = true,
                    .data = null,
                    .count = 1,
                };
            },
            else => error.InvalidResponse,
        };
    }

    fn executeDelete(self: *Query, ns: []const u8) !QueryResponse {
        const op = proto.Operation{
            .Delete = .{
                .store_ns = ns,
                .id = null, // Delete by filter
            },
        };

        const packet = try self.client.doOperation(op);
        defer proto.Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    return error.DeleteFailed;
                }
                break :blk QueryResponse{
                    .success = true,
                    .data = null,
                    .count = 1,
                };
            },
            else => error.InvalidResponse,
        };
    }

    fn executeQuery(self: *Query, ns: []const u8) !QueryResponse {
        // Build query JSON from AST
        const query_json = try self.ast.toJson(self.allocator);
        defer self.allocator.free(query_json);

        const op = proto.Operation{
            .Query = .{
                .store_ns = ns,
                .query_json = query_json,
            },
        };

        const packet = try self.client.doOperation(op);
        errdefer proto.Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    proto.Packet.free(self.allocator, packet);
                    return error.QueryFailed;
                }
                // Dupe data before freeing packet (reply.data is a slice into packet memory)
                const duped = if (reply.data) |d| try self.allocator.dupe(u8, d) else null;
                proto.Packet.free(self.allocator, packet);
                break :blk QueryResponse{
                    .success = true,
                    .data = duped,
                    .count = 0,
                    .allocator = self.allocator,
                };
            },
            else => {
                proto.Packet.free(self.allocator, packet);
                return error.InvalidResponse;
            },
        };
    }

    fn executeAggregate(self: *Query, ns: []const u8) !QueryResponse {
        // Build aggregate JSON from AST
        const agg_json = try self.ast.toJson(self.allocator);
        defer self.allocator.free(agg_json);

        const op = proto.Operation{
            .Aggregate = .{
                .store_ns = ns,
                .aggregate_json = agg_json,
            },
        };

        const packet = try self.client.doOperation(op);
        errdefer proto.Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    proto.Packet.free(self.allocator, packet);
                    return error.AggregateFailed;
                }
                // Dupe data before freeing packet (reply.data is a slice into packet memory)
                const duped = if (reply.data) |d| try self.allocator.dupe(u8, d) else null;
                proto.Packet.free(self.allocator, packet);
                break :blk QueryResponse{
                    .success = true,
                    .data = duped,
                    .count = 0,
                    .allocator = self.allocator,
                };
            },
            else => {
                proto.Packet.free(self.allocator, packet);
                return error.InvalidResponse;
            },
        };
    }

    fn executeScan(self: *Query, ns: []const u8, start_key: ?u128, record_limit: u32) !QueryResponse {
        // Use skip from query parameters (skip is managed by workload, not AST)
        const skip_count: u32 = 0;

        const op = proto.Operation{
            .Scan = .{
                .store_ns = ns,
                .start_key = start_key,
                .limit = record_limit,
                .skip = skip_count,
            },
        };

        const packet = try self.client.doOperation(op);
        errdefer proto.Packet.free(self.allocator, packet);

        return switch (packet.op) {
            .Reply => |reply| blk: {
                if (reply.status != .ok) {
                    proto.Packet.free(self.allocator, packet);
                    return error.ScanFailed;
                }
                // Dupe data before freeing packet (reply.data is a slice into packet memory)
                const duped = if (reply.data) |d| try self.allocator.dupe(u8, d) else null;
                proto.Packet.free(self.allocator, packet);
                break :blk QueryResponse{
                    .success = true,
                    .data = duped,
                    .count = 0,
                    .allocator = self.allocator,
                };
            },
            else => {
                proto.Packet.free(self.allocator, packet);
                return error.InvalidResponse;
            },
        };
    }

    pub fn deinit(self: *Query) void {
        self.ast.deinit();
        // TODO: Free mutation data if allocated
    }
};

/// Response from query execution
pub const QueryResponse = struct {
    success: bool,
    data: ?[]const u8 = null,
    count: usize = 0,
    allocator: ?Allocator = null,

    pub fn deinit(self: *QueryResponse) void {
        if (self.data) |d| {
            if (self.allocator) |alloc| {
                alloc.free(d);
            }
        }
    }
};
