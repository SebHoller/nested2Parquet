cmake_minimum_required(VERSION 3.10)

project(nested2Parquet)

find_package(fmt CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(RapidJSON CONFIG REQUIRED)

add_executable(nested2Parquet main.cpp)

target_link_libraries(nested2Parquet PRIVATE rapidjson)
target_link_libraries(nested2Parquet PRIVATE fmt::fmt)
target_link_libraries(nested2Parquet PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
target_link_libraries(nested2Parquet PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")