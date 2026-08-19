cmake_minimum_required(VERSION 3.10)
project(minirpc)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE Debug)

find_package(Protobuf REQUIRED)
include_directories(./ ${PROTOBUF_INCLUDE_DIRS})

set(PROTO_FILES proto/rpc_proto.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

aux_source_directory(src/util UTIL_SRC)
aux_source_directory(src/net NET_SRC)
aux_source_directory(src/codec CODEC_SRC)
aux_source_directory(src/rpc RPC_SRC)

# 服务端
add_executable(server
    server_main.cc
    ${UTIL_SRC} ${NET_SRC} ${CODEC_SRC} ${RPC_SRC}
    ${PROTO_SRCS}
)
target_link_libraries(server ${PROTOBUF_LIBRARIES} pthread)

# 客户端
add_executable(client
    client_main.cc
    ${UTIL_SRC} ${NET_SRC} ${CODEC_SRC} ${RPC_SRC}
    ${PROTO_SRCS}
)
target_link_libraries(client ${PROTOBUF_LIBRARIES} pthread)

# qps
add_executable(minirpc-qps
    test/qps.cc
    ${UTIL_SRC} ${NET_SRC} ${CODEC_SRC} ${RPC_SRC}
    ${PROTO_SRCS}
)
target_link_libraries(minirpc-qps ${PROTOBUF_LIBRARIES} pthread)
