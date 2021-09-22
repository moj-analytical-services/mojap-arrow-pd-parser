# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

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
