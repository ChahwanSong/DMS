# CMake generated Testfile for 
# Source directory: /workspace/DMS/data_plane
# Build directory: /workspace/DMS/data_plane/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[test_file_chunker]=] "/workspace/DMS/data_plane/build/test_file_chunker")
set_tests_properties([=[test_file_chunker]=] PROPERTIES  _BACKTRACE_TRIPLES "/workspace/DMS/data_plane/CMakeLists.txt;27;add_test;/workspace/DMS/data_plane/CMakeLists.txt;0;")
add_test([=[test_transfer_manager]=] "/workspace/DMS/data_plane/build/test_transfer_manager")
set_tests_properties([=[test_transfer_manager]=] PROPERTIES  _BACKTRACE_TRIPLES "/workspace/DMS/data_plane/CMakeLists.txt;28;add_test;/workspace/DMS/data_plane/CMakeLists.txt;0;")
add_test([=[test_checksum]=] "/workspace/DMS/data_plane/build/test_checksum")
set_tests_properties([=[test_checksum]=] PROPERTIES  _BACKTRACE_TRIPLES "/workspace/DMS/data_plane/CMakeLists.txt;29;add_test;/workspace/DMS/data_plane/CMakeLists.txt;0;")
