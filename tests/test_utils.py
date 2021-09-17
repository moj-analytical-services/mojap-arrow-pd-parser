import pytest
from io import StringIO, BytesIO

from mojap_metadata import Metadata

from arrow_pd_parser.utils import (
    FileFormat,
    is_s3_filepath,
    infer_format_from_filepath,
    infer_file_format_from_meta,
    infer_file_format,
    FileFormatNotFound,
)


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("json", FileFormat.JSON),
        ("jsonl", FileFormat.JSON),
        ("ndjson", FileFormat.JSON),
        ("csv", FileFormat.CSV),
        ("parquet", FileFormat.PARQUET),
    ],
)
def test_file_format(input_str, expected):
    actual = FileFormat.from_string(input_str)
    assert actual == expected

    actual = FileFormat.from_string(input_str.upper())
    assert actual == expected


def test_is_s3_filepath():
    assert is_s3_filepath("s3://bucket/object.csv") is True
    assert is_s3_filepath("local/file.csv") is False
    assert is_s3_filepath(StringIO()) is False
    assert is_s3_filepath(BytesIO()) is False


@pytest.mark.parametrize(
    "filepath,expected",
    [
        ("a/file.csv", FileFormat.CSV),
        ("a/file.csv.gzip", FileFormat.CSV),
        ("a/file.json", FileFormat.JSON),
        ("a/file.ndjson", FileFormat.JSON),
        ("a/file.jsonl", FileFormat.JSON),
        ("a/file.parquet", FileFormat.PARQUET),
        ("a/file.snappy.parquet", FileFormat.PARQUET),
    ],
)
def test_infer_format_from_filepath(filepath, expected):
    actual = infer_format_from_filepath(filepath)
    assert actual == expected


@pytest.mark.parametrize(
    "file_format,expected",
    [
        ("csv", FileFormat.CSV),
        ("csv.gzip", FileFormat.CSV),
        ("json", FileFormat.JSON),
        ("ndjson", FileFormat.JSON),
        ("jsonl", FileFormat.JSON),
        ("parquet", FileFormat.PARQUET),
        ("snappy.parquet", FileFormat.PARQUET),
    ],
)
def test_infer_file_format_from_meta(file_format, expected):
    meta = {
        "name": "test",
        "columns": [{"name": "a", "type": "string"}],
        "file_format": file_format,
    }
    actual = infer_file_format_from_meta(meta)
    assert actual == expected

    metadata = Metadata.from_dict(meta)
    actual = infer_file_format_from_meta(metadata)
    assert actual == expected


def test_infer_file_format():
    filepath = "test.csv"
    meta = {
        "name": "test",
        "columns": [{"name": "a", "type": "string"}],
        "file_format": "parquet",
    }
    actual = infer_file_format(filepath, meta)
    assert actual == FileFormat.CSV

    actual = infer_file_format(filepath, None)
    assert actual == FileFormat.CSV

    actual = infer_file_format("file.unknown", meta)
    assert actual == FileFormat.PARQUET

    with pytest.raises(FileFormatNotFound):
        _ = infer_file_format("file.unknown", None)

    meta["file_format"] = "bob"
    with pytest.raises(FileFormatNotFound):
        _ = infer_file_format("file.unknown", meta)

    _ = meta.pop("file_format")
    with pytest.raises(FileFormatNotFound):
        _ = infer_file_format("file.unknown", meta)
