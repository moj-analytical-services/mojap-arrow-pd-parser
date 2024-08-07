# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).


## 2.2.0 2024-08-08

- Added ability to raise boolean errors during type cast conversions in `cast_pandas_column_to_schema` and `cast_pandas_table_to_schema`.
- Expanded basic boolean mapper.
- Added additional test cases for the above

## 2.1.0 2024-04-24

- Fixed cast_pandas_table_to_schema to handle drop and ignore columns correctly
- Added additional test cases for the above

## 2.0.1 2023-11-22

- use Pypi Trusted Publisher action

## 2.0.0 2023-11-16

- Updates dependencies for security
- Updates tests now that pyarrow 14 handles date(time) granularity correctly (potentially breaking change, hence the major version number)

## 1.3.9 2023-07-25

- Loosens requirements to allow python 3.11

## 1.3.8 2023-06-13

- Fixes issue with checking parquet schema following write
  (see [this issue](https://github.com/moj-analytical-services/mojap-arrow-pd-parser/issues/96)). See PR [99](https://github.com/moj-analytical-services/mojap-arrow-pd-parser/pull/99) for more
  information.
- Allows ability to ignore unnamed columns on using the pandas csv
  reader by setting `ignore_unnamed_columns` to `True`. See PR [98](https://github.com/moj-analytical-services/mojap-arrow-pd-parser/pull/99) for more information.

## 1.3.4 2023-01-03

- Adds warning if parquet writer writes a file with different schema from the provided metadata (see [this issue](https://github.com/apache/arrow/issues/15032))
## 1.3.3 2022-12-19

- fixing bug `"pyarrow has no attribute 'parquet'"` (#87)
## 1.3.2 2022-10-07
- Updated dependencies.

## 1.3.1 2022-07-29
- Faster date casting.

## 1.3.0 2022-07-13
- Release for compatibility with code using numpy>1.22 for security reasons. As a result Python 3.7 is no longer supported.

## 1.2.1 2022-07-05
- fixed the ability to use relative local paths due to from_uri not accepting relative paths

## 1.2.0 2022-06-27
- Reader/writer engine choice added. Provide the reader_engine parameter to reader.read() or the writer_engine to writer.write() to specify a particular reader for a given file or format.

## 1.1.0 2022-05-12
- Chunked reads are now possible; use chunksize as a parameter to reader.read to return an iterator of pandas dataframes, useful for dealing with files that would otherwise exceed memory. This iterator can be used as an input to writer.write.

## 1.0.4 2022-03-25
- Dataframes are no longer mutated when writing

## 1.0.3 2021-11-29
- made more consistent the use of type_category so that if it wasn't present when casting data it would be created

## 1.0.2 2021-09-29
- the writer now makes the folder path it is writing to if it does not exist when writing locally
- added test for above addition

## 1.0.1 - 2021-09-27

- Removed `tests/__init__.py` not needed anymore.
- Made the `File_Format.from_string()` a bit more expansive. Just now checks if `PARQUET`, `JSON` or `CSV` are in the string (in that order).

## 1.0.0 - 2021-09-22

First Major Release

- Changed API for reader and writer to simplify entry point to readers and writers
- Reduced support for multiple readers currently only uses Pandas for CSV and JSONL and Arrow for Parquet. Codebase can be expanded in the future.
- Automatically reads from S3 or local depending on string filepath
- README.md discusses use of package. Changes are not backwards compatible.
- Pushed arrow dependency to at least version `5.0.0`. This is to reflect changes made in the parquet reader that is necessary for packages in our other eco systems.

## 0.4.4 - 2021-06-08

- made it so partition data is not cast

## 0.4.2 - 2021-06-08

- added warning for complex data types not being cast (no current support)

## 0.4.1 - 2021-04-29

- Added new Exception class should make it easier to run try/except as users can catch the new Exception class (`PandasCastError`)

## 0.4.0 - 2021-04-28

- Expanded parse module to `arrow_parser` and added a `pandas_parser` module. But import the previous function in the `__init__.py` of parse so that it is backwards compatible with previous releases.
- `pandas_parser` module added to give users option of arrow reader or pandas (for CSV and JSONL only).
- Added some basic tests for the pandas_parser

## 0.2.0 - 2021-03-16

- Updated docs
- Added skipped test which demonstrates known error in package (to be addressed in future update)
- Removed default indent of 4 from jsonl writer
- Updated the dependency for pyarrow allowing v2 and v3
- Upgraded the dependency for Pandas
