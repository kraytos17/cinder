# gRPC gateway (opt-in via CINDER_ENABLE_GRPC): third-party flags, protobuf +
# gRPC code generation for cinder/v1/cache.proto, and the cinder_proto /
# cinder_grpc libraries. Included from the top-level CMakeLists.txt; the
# option guard lives here so the call site is a single include.

include_guard(GLOBAL)

if(CINDER_ENABLE_GRPC)
    set(gRPC_BUILD_TESTS          OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CODEGEN        ON  CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_CPP_PLUGIN ON CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CPP_PLUGIN     OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CSHARP_EXT     OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_NODE_EXT       OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_PYTHON_EXT     OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_RUBY_EXT       OFF CACHE BOOL "" FORCE)
    set(gRPC_INSTALL              OFF CACHE BOOL "" FORCE)
    set(gRPC_SSL_PROVIDER         package CACHE STRING "" FORCE)
    set(protobuf_BUILD_TESTS      OFF CACHE BOOL "" FORCE)
    set(protobuf_INSTALL           OFF CACHE BOOL "" FORCE)
    set(absl_BUILD_TESTING        OFF CACHE BOOL "" FORCE)
    set(absl_PROPAGATE_CXX_STD    ON  CACHE BOOL "" FORCE)
    set(RE2_BUILD_TESTING         OFF CACHE BOOL "" FORCE)

    cinder_declare_dep(grpc WITH_SUBMODULES)
    # Suppress compiler warnings from gRPC internal source code (deprecated
    # c-ares APIs, missing-return in debug-only switch arms, attribute warnings
    # on unions, etc.). These are gRPC bugs, not ours.
    set(_cinder_save_cxx_flags "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
    FetchContent_MakeAvailable(grpc)
    set(CMAKE_CXX_FLAGS "${_cinder_save_cxx_flags}")

    # Compile proto/cinder/v1/cache.proto. protoc runs from the source root
    # so outputs land in a cinder/v1/ subtree mirroring the #include paths
    # used by the gateway, the service and the tests ("cinder/v1/...").
    set(_proto_src_dir ${CMAKE_CURRENT_SOURCE_DIR})
    set(_proto_out_dir ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${_proto_out_dir}/cinder/v1)

    set(_proto_file cinder/v1/cache.proto)
    add_custom_command(
        OUTPUT
            ${_proto_out_dir}/cinder/v1/cache.pb.cc
            ${_proto_out_dir}/cinder/v1/cache.pb.h
            ${_proto_out_dir}/cinder/v1/cache.grpc.pb.cc
            ${_proto_out_dir}/cinder/v1/cache.grpc.pb.h
        COMMAND $<TARGET_FILE:protoc>
            --cpp_out=${_proto_out_dir}
            --grpc_out=${_proto_out_dir}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>
            -I ${_proto_src_dir}
            ${_proto_file}
        DEPENDS ${_proto_src_dir}/${_proto_file} protoc grpc_cpp_plugin
        COMMENT "Generating protobuf + gRPC code from cache.proto"
        VERBATIM
    )

    add_library(cinder_proto OBJECT
        ${_proto_out_dir}/cinder/v1/cache.pb.cc
        ${_proto_out_dir}/cinder/v1/cache.grpc.pb.cc
    )
    target_include_directories(cinder_proto PUBLIC ${_proto_out_dir})
    target_link_libraries(cinder_proto PUBLIC
        protobuf::libprotobuf
        grpc++
        grpc
    )
    target_compile_options(cinder_proto PRIVATE -w)

    # cinder_grpc: gateway library (links cinder_core + proto + grpc++)
    add_library(cinder_grpc OBJECT
        src/net/grpc_gateway.cpp
        src/net/cache_service_impl.cpp
    )
    # Mirror CINDER_ENABLE_TLS handling on cinder_core: the source guards
    # (#ifdef CINDER_ENABLE_GRPC) need the macro as a compile definition, not
    # just the CMake option. Without this the gateway, the daemon wiring and
    # the tests silently compile out even in grpc-enabled presets.
    target_compile_definitions(cinder_grpc PUBLIC CINDER_ENABLE_GRPC)
    target_include_directories(cinder_grpc PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${_proto_out_dir}
    )
    target_link_libraries(cinder_grpc PUBLIC cinder_core cinder_proto)
    target_compile_options(cinder_grpc PRIVATE -w)
endif()
