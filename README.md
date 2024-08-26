# nested2Parquet

nested2Parquet is a robust and memory-efficient parser for nested JSON to Apache Parquet files

This project is my Master's Thesis at Technical University Munich (TUM)

## Development

This project uses vcpkg and CMake for compilation.

vcpkg can be cloned from [here](https://github.com/microsoft/vcpkg)

## Supported data types

This parser is able to convert the following data types in JSON to their corresponding representation in Parquet:

- Null
- Boolean
- String
- String with format date
- Integer (32 and 64 Bit)
- Double
