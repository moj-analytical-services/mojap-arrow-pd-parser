# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## 0.3.0 - 2021-04-26

- Expanded parse module to `arrow_parser` and added a `pandas_parser` module. But import the previous function in the `__init__.py` of parse so that it is backwards compatible with previous releases.
- `pandas_parser` module added to give users option of arrow reader or pandas (for CSV and JSONL (JSONL not yet implemented but will be added in minor update)).
- Added some basic tests for the pandas_parser


## 0.2.0 - 2021-03-16

- Updated docs
- Added skipped test which demonstrates known error in package (to be addressed in future update)
- Removed default indent of 4 from jsonl writer
- Updated the dependency for pyarrow allowing v2 and v3
- Upgraded the dependency for Pandas
