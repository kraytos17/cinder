function(cinder_add_tool name source)
    add_executable(${name} ${source})
    target_link_libraries(${name} PRIVATE cinder::core CLI11::CLI11)
    set_target_properties(${name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        CXX_STANDARD 23
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
    )
    _cinder_apply_debug_options(${name})
    cinder_reuse_pch(${name})
endfunction()

cinder_add_tool(cinderd cinderd_main.cpp)
if(CINDER_ENABLE_GRPC)
    target_link_libraries(cinderd PRIVATE cinder_grpc cinder_proto)
endif()

cinder_add_tool(cinder-cli cinder_cli.cpp)
cinder_add_tool(cinder-admin cinder_admin.cpp)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} PREFIX tools FILES
    cinderd_main.cpp cinder_cli.cpp cinder_admin.cpp)
