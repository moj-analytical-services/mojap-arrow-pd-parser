from io import BytesIO, StringIO

import pytest
from arrow_pd_parser.utils import (
    FileFormat,
    FileFormatNotFound,
    infer_file_format,
    infer_file_format_from_filepath,
    infer_file_format_from_meta,
    is_s3_filepath,
)
from mojap_metadata import Metadata

test_file_types = [
    ("json", FileFormat.JSON),
    ("jsonl", FileFormat.JSON),
    ("ndjson", FileFormat.JSON),
    ("csv", FileFormat.CSV),
    ("csv.gzip", FileFormat.CSV),
    ("parquet", FileFormat.PARQUET),
    ("SNAPPY.PARQUET", FileFormat.PARQUET),
]

test_filenames = [(f"file.{x[0]}", x[1]) for x in test_file_types]


@pytest.mark.parametrize(
    "file_name,expected",
    test_filenames,
)
class Test_infer_file_format:
    def test_fileformat_from_string_filename(self, file_name, expected):
        actual = FileFormat.from_string(file_name)
        assert actual == expected

    def test_fileformat_from_string_upper(self, file_name, expected):
        actual = FileFormat.from_string(file_name.upper())
        assert actual == expected

    def test_fileformat_from_string_from_filepath(self, file_name, expected):
        actual = FileFormat.from_string(f"file_path/{file_name}")
        assert actual == expected

    def test_infer_file_format_filename(self, file_name, expected):
        actual = infer_file_format(file_name)
        assert actual == expected

    def test_infer_file_format_upper(self, file_name, expected):
        actual = infer_file_format(file_name.upper())
        assert actual == expected

    def test_infer_file_format_from_filepath(self, file_name, expected):
        actual = infer_file_format_from_filepath(f"file_path/{file_name}")
        assert actual == expected


def test_is_s3_filepath():
    assert is_s3_filepath("s3://bucket/object.csv") is True
    assert is_s3_filepath("local/file.csv") is False
    assert is_s3_filepath(StringIO()) is False
    assert is_s3_filepath(BytesIO()) is False


def generate_meta(file_format: str):
    return {
        "name": "test",
        "columns": [{"name": "a", "type": "string"}],
        "file_format": file_format,
    }


@pytest.mark.parametrize(
    "file_format,expected",
    test_file_types,
)
class Test_infer_file_format_from_meta:
    def test_infer_file_format_direct_from_meta(self, file_format, expected):
        meta = generate_meta(file_format)
        actual = infer_file_format_from_meta(meta)
        assert actual == expected

    def test_infer_file_from_metadata_meta(self, file_format, expected):
        meta = generate_meta(file_format)
        metadata = Metadata.from_dict(meta)
        actual = infer_file_format_from_meta(metadata)
        assert actual == expected


@pytest.mark.parametrize(
    "file_name,expected",
    test_filenames,
)
class Test_infer_file_format_with_meta:
    def test_conflict_infer_from_both(self, file_name, expected):
        meta = generate_meta(file_format="parquet")
        actual = infer_file_format(file_name, meta)
        assert actual == expected

    def test_conflict_infer_from_filepath(self, file_name, expected):
        actual = infer_file_format(file_name, None)
        assert actual == expected


class Test_infer_file_format_with_meta_conflict:
    def test_infer_from_unknown_with_meta(self):
        meta = generate_meta(file_format="parquet")
        actual = infer_file_format("file.unknown", meta)
        assert actual == FileFormat.PARQUET

    def test_infer_from_conflict_name(self):
        actual = infer_file_format("from_parquet.csv", None)
        assert actual == FileFormat.CSV

        actual_dots = infer_file_format("from.original.parquet.file.csv", None)
        assert actual_dots == FileFormat.CSV

    def test_missing_file_format_raises_error(self):
        meta = generate_meta(file_format="bob")

        with pytest.raises(FileFormatNotFound):
            infer_file_format("file.unknown", None)

        with pytest.raises(FileFormatNotFound):
            _ = infer_file_format("file.unknown", meta)

        _ = meta.pop("file_format")
        with pytest.raises(FileFormatNotFound):
            _ = infer_file_format("file.unknown", meta)
