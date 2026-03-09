const std = @import("std");


pub fn build(b: *std.Build) void {
 
    const target = b.standardTargetOptions(.{});
    
    const optimize = b.standardOptimizeOption(.{});
   
    const proto = b.dependency("proto", .{});
    const bson = b.dependency("bson", .{});

  
    const mod = b.addModule("shinydb_zig_client", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "proto", .module = proto.module("proto") },
            .{ .name = "bson", .module = bson.module("bson") },
        },
    });

    const exe = b.addExecutable(.{
        .name = "shinydb_zig_client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),         
            .target = target,
            .optimize = optimize,           
            .imports = &.{    
                .{ .name = "shinydb_zig_client", .module = mod },
            },
        }),
    });

    b.installArtifact(exe);

    

    
    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

   
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);


    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/integration_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "shinydb_zig_client", .module = mod },
                .{ .name = "proto", .module = proto.module("proto") },
                .{ .name = "bson", .module = bson.module("bson") },
            },
        }),
    });

    const run_integration_tests = b.addRunArtifact(integration_tests);

    const builder_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/builder_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "shinydb_zig_client", .module = mod },
                .{ .name = "proto", .module = proto.module("proto") },
            },
        }),
    });
    const run_builder_tests = b.addRunArtifact(builder_tests);

    const client_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/client_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "shinydb_zig_client", .module = mod },
            },
        }),
    });
    const run_client_tests = b.addRunArtifact(client_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);
    test_step.dependOn(&run_builder_tests.step);
    test_step.dependOn(&run_client_tests.step);

    
    const integration_step = b.step("test-integration", "Run integration tests (requires running server)");
    integration_step.dependOn(&run_integration_tests.step);

}
