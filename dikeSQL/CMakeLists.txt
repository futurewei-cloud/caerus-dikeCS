cmake_minimum_required(VERSION 3.0.0)
project(dikeSQL VERSION 0.1.0)


add_library(libsqlite STATIC sqlite3.c csv.c tbl.c sqlite3ext.h sqlite3.h)

include_directories(.)

target_compile_options(libsqlite PRIVATE 
#        -DSQLITE_THREADSAFE=1 
        -DSQLITE_THREADSAFE=0
        -DSQLITE_ENABLE_MEMSYS5 
#        -DSQLITE_ENABLE_JSON1 
#        -DSQLITE_ENABLE_RTREE 
#        -DSQLITE_ENABLE_EXPLAIN_COMMENTS 
        -DHAVE_USLEEP -DSQLITE_CORE -DSQLITE_OMIT_LOAD_EXTENSION=1 
        -fmax-errors=1)


add_executable(dikeSQL testDikeSQL.cpp)
target_link_libraries(dikeSQL PUBLIC  libsqlite "-lpthread -lm" )

