cmake_minimum_required(VERSION 3.20)
project(dms_data_plane LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(dms STATIC
    src/file_chunker.cpp
    src/network.cpp
    src/transfer_manager.cpp
)

target_include_directories(dms PUBLIC include)

enable_testing()

add_executable(test_file_chunker tests/test_file_chunker.cpp)
target_link_libraries(test_file_chunker PRIVATE dms)

add_executable(test_transfer_manager tests/test_transfer_manager.cpp)
target_link_libraries(test_transfer_manager PRIVATE dms)

add_test(NAME test_file_chunker COMMAND test_file_chunker)
add_test(NAME test_transfer_manager COMMAND test_transfer_manager)
