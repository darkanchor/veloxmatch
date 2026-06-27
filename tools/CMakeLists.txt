add_executable(wal_reader wal_reader.c)

target_include_directories(wal_reader
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(wal_reader
    PRIVATE
        openmatch
        ombus
)

add_executable(wal_maker wal_maker.c)

target_include_directories(wal_maker
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(wal_maker
    PRIVATE
        openmatch
)

find_package(SQLite3)
if(SQLite3_FOUND)
    add_library(wal_query MODULE wal_query.c)
    set_target_properties(wal_query PROPERTIES
        PREFIX ""
        OUTPUT_NAME "wal_query"
    )
    target_include_directories(wal_query
        PRIVATE
            ${CMAKE_SOURCE_DIR}/include
            ${SQLite3_INCLUDE_DIRS}
    )
    if(TARGET SQLite3::SQLite3)
        target_link_libraries(wal_query PRIVATE openmatch SQLite3::SQLite3)
    elseif(TARGET SQLite::SQLite3)
        target_link_libraries(wal_query PRIVATE openmatch SQLite::SQLite3)
    else()
        target_link_libraries(wal_query PRIVATE openmatch ${SQLite3_LIBRARIES})
    endif()
endif()
