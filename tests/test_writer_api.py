import io
import logging
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

import awswrangler as wr
import boto3
import pytest
from dataengineeringutils3.s3 import s3_path_to_bucket_key
from moto import mock_aws

from arrow_pd_parser import _writers, reader, writer
from arrow_pd_parser._writers import (
    ArrowCsvWriter,
    ArrowParquetWriter,
    PandasCsvWriter,
    PandasJsonWriter,
    get_writer_for_file_format,
)
from arrow_pd_parser.utils import EngineNotImplementedError

logging.getLogger("arrow_pd_parser").setLevel(logging.DEBUG)


pandas_writers = {**dict.fromkeys([PandasCsvWriter, PandasJsonWriter], "pandas")}
arrow_writers = {**dict.fromkeys([ArrowCsvWriter, ArrowParquetWriter], "arrow")}

writers = {**pandas_writers, **arrow_writers}

json_file_types = ["json", "jsonl", "ndjson"]
csv_file_types = ["csv", "csv.gzip"]
parquet_file_types = ["parquet", "SNAPPY.PARQUET"]

default_file_types = {
    PandasJsonWriter: json_file_types,
    PandasCsvWriter: csv_file_types,
    ArrowParquetWriter: parquet_file_types,
}

test_default_file_types = [
    [value, key, writers[key]]
    for key, list_of_values in default_file_types.items()
    for value in list_of_values
]

# Output format:
# test_default_file_types = [
#     ["json", PandasJsonWriter, "pandas"],
#     ...
#     ["SNAPPY.PARQUET", ArrowParquetWriter, "arrow"],
# ]
test_default_file_types_writer = [item[:-1] for item in test_default_file_types]

valid_file_types = {**default_file_types, ArrowCsvWriter: csv_file_types}

test_valid_file_types = [
    [value, key, writers[key]]
    for key, list_of_values in valid_file_types.items()
    for value in list_of_values
]

test_valid_file_types_writer = [item[:-1] for item in test_valid_file_types]

test_mismatch_file_types = [
    ["json", PandasCsvWriter],
    ["jsonl", PandasCsvWriter],
    ["ndjson", ArrowParquetWriter],
    ["csv", PandasJsonWriter],
    ["csv.gzip", ArrowParquetWriter],
    ["parquet", PandasJsonWriter],
    ["SNAPPY.PARQUET", PandasCsvWriter],
]

valid_engines = ["pandas", "arrow", None]

invalid_file_type_engine_combinations = [
    ["json", "arrow"],
    ["parquet", "pandas"],
]

invalid_engines = ["spark", "dplyr"]


class MockS3FilesystemReadInputStream:
    @staticmethod
    @contextmanager
    def open_input_stream(s3_file_path_in: str) -> io.BytesIO:
        s3_resource = boto3.resource("s3")
        bucket, key = s3_path_to_bucket_key(s3_file_path_in)
        obj_bytes = s3_resource.Object(bucket, key).get()["Body"].read()
        obj_io_bytes = io.BytesIO(obj_bytes)
        try:
            yield obj_io_bytes
        finally:
            obj_io_bytes.close()

    @staticmethod
    @contextmanager
    def open_input_file(s3_file_path_in: str):
        s3_client = boto3.client("s3")
        bucket, key = s3_path_to_bucket_key(s3_file_path_in)
        tmp_file = tempfile.NamedTemporaryFile(suffix=Path(key).suffix)
        s3_client.download_file(bucket, key, tmp_file.name)
        yield tmp_file.name


def mock_get_file(*args, **kwargs):
    return MockS3FilesystemReadInputStream()


class MockParquetWriter:
    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        return self

    @staticmethod
    def write_table(*args, **kwargs):
        pass


def mock_write_table(*args, **kwargs):
    return MockParquetWriter()


@pytest.mark.parametrize("data_format, expected_class", test_default_file_types_writer)
class Test_get_default_writer:
    def test_get_default_writer_type_from_file_format(
        self, data_format, expected_class
    ):
        """
        Test that get_writer_for_file_format retrieves the correct writer for
        each file format.
        """
        actual = get_writer_for_file_format(data_format, None)
        assert isinstance(actual, expected_class)

    def test_infer_default_writer_from_file_path(
        self, monkeypatch: pytest.MonkeyPatch, data_format, expected_class, df_all_types
    ):
        """
        Test that get_writer_for_file_format can infer the file type from the file name
        and then retrieve the correct writer for each file format, when no engine
        argument is supplied.
        """
        # stub out writer.expected_class.write for True (rather than None)
        # so actual --> True if the expected_class is called
        monkeypatch.setattr(expected_class, "write", lambda *args, **kwargs: True)

        file_name = f"file_name.{data_format}"
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = os.path.join(tmp_dir, file_name)

        actual = writer.write(df=df_all_types, output_path=output_path)

        assert actual


@pytest.mark.parametrize("data_format, unexpected_class", test_mismatch_file_types)
class Test_get_default_writer_error_if_mismatch:
    def test_error_if_mismatch_get_writer_type_for_file_format(
        self, data_format, unexpected_class
    ):
        """
        Test that get_writer_for_file_format retrieves a writer that is
        different from the incorrect class.
        """
        actual = get_writer_for_file_format(data_format)

        with pytest.raises(AssertionError):
            assert isinstance(actual, unexpected_class)

    def test_error_if_mismatch_infer_writer_from_file_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        data_format,
        unexpected_class,
        df_all_types,
    ):
        """
        Test that get_writer_for_file_format retrieves a writer that is
        different from the incorrect class when inferring file format from the file
        name.
        """
        # stub out writer.unexpected_class.write for True (rather than None)
        # so actual --> None, raising an AssertionError if the unexpected_class
        # is not called
        monkeypatch.setattr(unexpected_class, "write", lambda *args, **kwargs: True)
        file_name = f"file_name.{data_format}"

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = os.path.join(tmp_dir, file_name)

        actual = writer.write(df=df_all_types, output_path=output_path)

        with pytest.raises(AssertionError):
            assert actual


@pytest.mark.parametrize(
    "data_format, expected_class, writer_engine", test_valid_file_types
)
def test_get_default_writer_type_from_file_format(
    data_format, expected_class, writer_engine
):
    """
    Test that get_writer_for_file_format retrieves the correct writer for
    each valid combination of file_format and writer_engine.
    """
    actual = get_writer_for_file_format(data_format, writer_engine)
    assert isinstance(actual, expected_class)


@pytest.mark.parametrize(
    "data_format, expected_class, writer_engine", test_valid_file_types
)
def test_infer_writer_from_file_path(
    monkeypatch: pytest.MonkeyPatch,
    data_format,
    expected_class,
    writer_engine,
    df_all_types,
):
    """
    Test that get_writer_for_file_format can infer the file type from the file name
    and then retrieve the correct writer for each file format, when no engine
    argument is supplied.
    """
    # stub out writer.unexpected_class.write for True (rather than None)
    # so actual --> None, raising an AssertionError if the unexpected_class
    # is not called
    monkeypatch.setattr(expected_class, "write", lambda *args, **kwargs: True)
    file_name = f"file_name.{data_format}"

    with tempfile.TemporaryDirectory() as tmp_dir:
        output_path = os.path.join(tmp_dir, file_name)

    actual = writer.write(
        df=df_all_types, output_path=output_path, writer_engine=writer_engine
    )

    assert actual


@pytest.mark.parametrize(
    "data_format", [*csv_file_types, *json_file_types, *parquet_file_types]
)
@pytest.mark.parametrize("writer_engine", invalid_engines)
def test_error_if_mismatch_engine_get_writer_for_file_format(
    data_format, writer_engine
):
    """
    Test that get_writer_for_file_format raises a EngineNotImplementedError if supplied
    an invalid / not implented engine.
    """
    with pytest.raises(EngineNotImplementedError):
        get_writer_for_file_format(data_format, writer_engine)


@pytest.mark.parametrize("data_format", test_default_file_types)
def test_no_error_when_write_local_path_not_exist(data_format):
    """
    Test that if the path does not exist, the writer will not error
    """
    #
    df = reader.read("tests/data/all_types.csv")
    file_path = f"does/not/exist/data.{data_format}"

    with tempfile.TemporaryDirectory() as tmp_dir:
        out_file = os.path.join(tmp_dir, file_path)

    writer.write(df, out_file)


@mock_aws()
def test_read_parquet_schema_on_write_to_s3(df_all_types, monkeypatch):
    s3_client = boto3.client("s3")

    _ = s3_client.create_bucket(
        Bucket="my-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    with tempfile.NamedTemporaryFile(suffix=".snappy.parquet") as tmp:
        writer.write(df_all_types, tmp.name)
        schema = _writers.pq.read_schema(tmp.name)
        output_path = f"s3://my-bucket/{Path(tmp.name).name}"
        wr.s3.upload(tmp.name, output_path)

    _ = monkeypatch.setattr(_writers.fs, "S3FileSystem", mock_get_file)
    _ = monkeypatch.setattr(_writers.pq, "ParquetWriter", mock_write_table)

    _ = _writers.ArrowParquetWriter()._write(
        df=iter([df_all_types]), output_path=output_path, arrow_schema=schema
    )
