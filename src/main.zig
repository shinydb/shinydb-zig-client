const std = @import("std");
const Io = std.Io;
const shinydb = @import("shinydb_zig_client");
const ShinyDbClient = shinydb.ShinyDbClient;
const Query = shinydb.Query;

pub fn main() !void {
    // Setup allocator
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    defer _ = debug_allocator.deinit();
    const gpa = debug_allocator.allocator();

    // Setup I/O
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Create client
    var client = try ShinyDbClient.init(gpa, io);
    defer client.deinit();

    // Connect to shinydb server
    std.debug.print("Connecting to shinydb server at 127.0.0.1:23469...\n", .{});
    client.connect("127.0.0.1", 23469) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        std.debug.print("Make sure shinydb server is running on port 23469\n", .{});
        return;
    };
    defer client.disconnect();
    std.debug.print("Connected! Session ID: {}\n\n", .{client.session_id});

    // Authenticate
    std.debug.print("=== Authenticating ===\n", .{});
    var auth_result = try client.authenticate("admin", "admin");
    defer auth_result.deinit();
    std.debug.print("✓ Authenticated successfully\n\n", .{});

    // Example 1: Create a space using unified API
    std.debug.print("=== Example 1: Create Space ===\n", .{});
    client.create(shinydb.Space{
        .id = 0,
        .ns = "test_app",
        .description = "Test application space",
        .created_at = 0,
    }) catch {
        std.debug.print("(Space 'test_app' may already exist)\n", .{});
    };
    std.debug.print("✓ Space 'test_app' ready\n\n", .{});

    // Example 2: Create a store (auto-creates space if needed)
    std.debug.print("=== Example 2: Create Store ===\n", .{});
    client.create(shinydb.Store{
        .id = 0,
        .store_id = 0,
        .ns = "test_app.users",
        .description = "User data store",
        .created_at = 0,
    }) catch {
        std.debug.print("(Store 'test_app.users' may already exist)\n", .{});
    };
    std.debug.print("✓ Store 'test_app.users' ready\n\n", .{});

    // Example 3: Create a document using Query builder
    std.debug.print("=== Example 3: Create Document ===\n", .{});
    var query = Query.init(client);
    defer query.deinit();

    const User = struct {
        name: []const u8,
        age: u32,
        email: []const u8,
    };
    const user_data = User{
        .name = "Alice",
        .age = 30,
        .email = "alice@example.com",
    };
    _ = try query.space("test_app").store("users").create(user_data);
    var insert_response = try query.run();
    defer insert_response.deinit();
    if (insert_response.data) |data| {
        std.debug.print("✓ Created document: {s}\n\n", .{data});
    } else {
        std.debug.print("✓ Document created successfully\n\n", .{});
    }

    // Example 4: Query documents using Query builder
    std.debug.print("=== Example 4: Query Documents ===\n", .{});
    var query2 = Query.init(client);
    defer query2.deinit();

    _ = query2.space("test_app").store("users").limit(5);
    var response = query2.run() catch |err| {
        std.debug.print("Query failed: {}\n", .{err});
        return err;
    };
    defer response.deinit();

    if (response.data) |data| {
        std.debug.print("Query result: {s}\n\n", .{data});
    } else {
        std.debug.print("Query returned no data\n\n", .{});
    }

    // Example 5: List spaces
    std.debug.print("=== Example 5: List Spaces ===\n", .{});
    const spaces = client.list(.Space, null) catch |err| {
        std.debug.print("Failed to list spaces: {}\n", .{err});
        return err;
    };
    defer gpa.free(spaces);
    std.debug.print("Spaces: {s}\n\n", .{spaces});

    // Example 6: Flush to disk
    std.debug.print("=== Example 6: Flush Database ===\n", .{});
    client.flush() catch |err| {
        std.debug.print("Failed to flush: {}\n", .{err});
        return;
    };
    std.debug.print("✓ Database flushed to disk\n\n", .{});

    std.debug.print("✅ All operations completed successfully!\n", .{});
}
