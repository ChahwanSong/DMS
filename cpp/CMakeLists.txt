cmake_minimum_required(VERSION 3.16)
project(dms_dataplane LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_executable(dms_tcp_transfer src/tcp_transfer.cpp)

if (MSVC)
  target_compile_definitions(dms_tcp_transfer PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()
