import tempfile

import numpy as np
import pandas as pd
import pytest
from arrow_pd_parser import reader, writer
from arrow_pd_parser._readers import (
    ArrowCsvReader,
    ArrowParquetReader,
    PandasCsvReader,
    PandasJsonReader,
    get_reader_for_file_format,
)
from arrow_pd_parser.utils import FileFormat, infer_file_format_from_filepath
from pandas.testing import assert_frame_equal

pandas_readers = {**dict.fromkeys([PandasCsvReader, PandasJsonReader], "pandas")}
arrow_readers = {**dict.fromkeys([ArrowCsvReader, ArrowParquetReader], "arrow")}

readers = {**pandas_readers, **arrow_readers}

json_file_types = ["json", "jsonl", "ndjson"]
csv_file_types = ["csv", "csv.gzip"]
parquet_file_types = ["parquet", "SNAPPY.PARQUET"]

default_file_types = {
    PandasJsonReader: json_file_types,
    PandasCsvReader: csv_file_types,
    ArrowParquetReader: parquet_file_types,
}

test_default_file_types = [
    [value, key, readers[key]]
    for key, list_of_values in default_file_types.items()
    for value in list_of_values
]

# Output format:
# test_default_file_types = [
#     ["json", PandasJsonReader, "pandas"],
#     ...
#     ["SNAPPY.PARQUET", ArrowParquetReader, "arrow"],
# ]
test_default_file_types_reader = [item[:-1] for item in test_default_file_types]

valid_file_types = {**default_file_types, ArrowCsvReader: ["csv"]}

test_valid_file_types = [
    [value, key, readers[key]]
    for key, list_of_values in valid_file_types.items()
    for value in list_of_values
]

test_valid_file_types_reader = [item[:-1] for item in test_valid_file_types]

test_mismatch_file_types = [
    ["json", PandasCsvReader],
    ["jsonl", PandasCsvReader],
    ["ndjson", ArrowParquetReader],
    ["csv", PandasJsonReader],
    ["csv.gzip", ArrowParquetReader],
    ["parquet", PandasJsonReader],
    ["SNAPPY.PARQUET", PandasCsvReader],
]

valid_engines = ["pandas", "arrow", None]

invalid_file_type_engine_combinations = [
    ["json", "arrow"],
    ["parquet", "pandas"],
]

invalid_engines = ["spark", "dplyr"]


@pytest.mark.parametrize("data_format", ["jsonl", "csv"])
def test_inferred_cols_pandas_types(data_format):
    df = reader.read(f"tests/data/all_types.{data_format}")
    test = df.dtypes.to_dict()

    assert isinstance(test["i"], pd.core.arrays.integer.Int64Dtype)
    assert isinstance(test["my_float"], type(np.dtype("float64")))
    assert isinstance(test["my_bool"], pd.core.arrays.boolean.BooleanDtype)
    assert isinstance(test["my_string"], pd.core.arrays.string_.StringDtype)

    if data_format == "jsonl":
        pytest.skip("Pandas cannot infer bool with nulls from JSON datasets")
    else:
        assert isinstance(test["my_nullable_bool"], pd.core.arrays.boolean.BooleanDtype)


@pytest.mark.parametrize("data_format, expected_class", test_default_file_types_reader)
class Test_get_default_reader:
    def test_get_default_reader_type_from_file_format(
        self, data_format, expected_class
    ):
        """
        Test that get_reader_for_file_format retrieves the correct reader for
        each file format.
        """
        actual = get_reader_for_file_format(data_format, None)
        assert isinstance(actual, expected_class)

    @pytest.mark.parametrize("use_meta", [True, False])
    def test_infer_default_reader_from_file_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        data_format,
        expected_class,
        use_meta,
        test_meta,
    ):
        """
        Test that get_reader_for_file_format can infer the file type from the file name
        and then retrieve the correct reader for each file format, when no engine
        argument is supplied.
        """
        if use_meta:
            meta = test_meta
        else:
            meta = None

        with tempfile.NamedTemporaryFile(suffix="." + data_format) as temp_file_name:
            temp_out_file = temp_file_name.name

        # stub out reader.expected_class.read for True (rather than pd.DataFrame)
        # so actual --> True if the expected_class is called
        monkeypatch.setattr(expected_class, "read", lambda *args, **kwargs: "stub")

        actual = reader.read(input_path=temp_out_file, metadata=meta)

        assert actual == "stub"


@pytest.mark.parametrize("data_format, unexpected_class", test_mismatch_file_types)
class Test_get_default_reader_error_if_mismatch:
    def test_error_if_mismatch_get_reader_type_for_file_format(
        self, data_format, unexpected_class
    ):
        """
        Test that get_reader_for_file_format retrieves a reader that is
        different from the incorrect class.
        """
        actual = get_reader_for_file_format(data_format)

        with pytest.raises(AssertionError):
            assert isinstance(actual, unexpected_class)

    @pytest.mark.parametrize("use_meta", [True, False])
    def test_error_if_mismatch_infer_reader_from_file_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        data_format: str,
        unexpected_class,
        use_meta: bool,
        test_meta,
        df_all_types,
    ):
        """
        Test that get_reader_for_file_format retrieves a reader that is
        different from the incorrect class when inferring file format from the file
        name.
        """
        if use_meta:
            meta = test_meta
        else:
            meta = None

        with tempfile.NamedTemporaryFile(suffix="." + data_format) as temp_file_name:
            temp_out_file = temp_file_name.name

        writer.write(df=df_all_types, output_path=temp_out_file)
        # stub out reader.unexpected_class.read for "stub" (rather than pd.DataFrame)
        # so actual --> "stub", raising an AssertionError if the unexpected_class
        # is not called
        monkeypatch.setattr(unexpected_class, "read", lambda *args, **kwargs: "stub")

        test_data_path = temp_out_file

        kwargs = {}
        if infer_file_format_from_filepath(temp_out_file) == FileFormat.PARQUET:
            kwargs["parquet_expect_full_schema"] = False
        actual = reader.read(input_path=test_data_path, metadata=meta, **kwargs)

        with pytest.raises(AssertionError):
            assert isinstance(actual, str)


@pytest.mark.parametrize("data_format", ["jsonl", "csv"])
@pytest.mark.parametrize("use_meta", [True, False])
def test_default_read(data_format, use_meta, test_meta, df_all_types):
    if use_meta:
        meta = test_meta
    else:
        meta = None

    with tempfile.NamedTemporaryFile(suffix="." + data_format) as temp_file_name:
        temp_out_file = temp_file_name.name

    writer.write(df=df_all_types, output_path=temp_out_file)

    df_default_inferred = reader.read(input_path=temp_out_file, metadata=meta)

    if data_format == "csv":
        df_default_specified = reader.csv.read(input_path=temp_out_file, metadata=meta)
    elif data_format == "jsonl":
        df_default_specified = reader.json.read(input_path=temp_out_file, metadata=meta)
    else:
        raise ValueError(f"Test wasn't expecting: {data_format}")

    assert_frame_equal(df_default_inferred, df_default_specified)


@pytest.mark.parametrize(
    "data_format, supplied_reader, reader_engine", test_valid_file_types
)
@pytest.mark.parametrize("use_meta", [True, False])
def test_reader_chunked(
    data_format, supplied_reader, reader_engine, use_meta, test_meta, df_all_types
):
    if use_meta:
        meta = test_meta
    else:
        meta = None

    with tempfile.NamedTemporaryFile(suffix="." + data_format) as temp_file_name:
        temp_out_file = temp_file_name.name

    writer.write(df=df_all_types, output_path=temp_out_file, file_format=data_format)
    kwargs = {}
    if infer_file_format_from_filepath(temp_out_file) == FileFormat.PARQUET:
        kwargs["parquet_expect_full_schema"] = False

    df_unchunked = reader.read(
        input_path=temp_out_file, metadata=meta, reader_engine=reader_engine, **kwargs
    )
    df_chunked_generator = reader.read(
        input_path=temp_out_file,
        metadata=meta,
        reader_engine=reader_engine,
        chunksize=2,
        **kwargs,
    )

    df_chunked = pd.concat(df_chunked_generator, ignore_index=True)

    if not meta and reader_engine == "pandas":
        assert_frame_equal(
            left=df_unchunked,
            right=df_chunked,
            check_dtype=False,  # chunked reader generates pd.Float64 columns rather than np.float64 columns # noqa E501
        )
    else:
        assert_frame_equal(df_unchunked, df_chunked)


# other round trips are tested in tests/test_round_trip.py
