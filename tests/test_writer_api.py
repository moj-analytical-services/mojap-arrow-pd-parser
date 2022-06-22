import logging
import os
import tempfile

import pytest
from arrow_pd_parser import reader, writer
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
