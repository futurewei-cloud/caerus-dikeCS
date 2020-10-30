cmake_minimum_required(VERSION 3.10)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries")
add_subdirectory(external/poco)
add_subdirectory(dikeSQL)

include_directories(dikeSQL)

project(dikeS3)
add_executable(dikeCS dikeCS.cpp)

target_include_directories(dikeCS PUBLIC 
    ${CMAKE_SOURCE_DIR}/external/poco/Foundation/include 
    ${CMAKE_SOURCE_DIR}/external/poco/Util/include
    ${CMAKE_SOURCE_DIR}/external/poco/Net/include 
    ${CMAKE_SOURCE_DIR}/external/build-aws/include
    )

target_link_libraries(dikeCS PUBLIC 
    Poco::Foundation
    Poco::Util
    Poco::Net
    libsqlite
    "-lpthread -lm"
    "-L${CMAKE_SOURCE_DIR}/external/build-aws/lib -laws-c-event-stream  -laws-checksums -laws-c-common"
    )
