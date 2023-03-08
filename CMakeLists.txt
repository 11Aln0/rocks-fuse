cmake_minimum_required(VERSION 3.25)
project(rocks_fuse)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall")


set(ROCKSDB_INCLUDE_DIR "/usr/local/rocksdb/include")
set(FUSE_INCLUDE_DIR "/usr/local/include/fuse3")
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h PATHS)

include_directories(${ROCKSDB_INCLUDE_DIR} ${FUSE_INCLUDE_DIR})

find_library(ROCKSDB_LIB rocksdb)
set(FUSE_LIB "/usr/lib/x86_64-linux-gnu/libfuse3.so.3.10.5")


add_executable(rocks_fuse
        rfs.cpp types.h rfs.h rocksdb_fs.cpp rocksdb_fs_utils.cpp)
target_link_libraries(rocks_fuse ${ROCKSDB_LIB} ${FUSE_LIB})