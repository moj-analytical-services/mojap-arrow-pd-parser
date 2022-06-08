import tempfile

import pytest
from arrow_pd_parser import reader, writer
from arrow_pd_parser.utils import FileFormat
from pandas.testing import assert_frame_equal

all_formats = [FileFormat.CSV, FileFormat.JSON, FileFormat.PARQUET]

csv_file_types = {FileFormat.CSV: ["csv", "csv.gzip"]}
json_file_types = {FileFormat.JSON: ["json", "jsonl", "ndjson"]}
parquet_file_types = {FileFormat.PARQUET: ["parquet", "SNAPPY.PARQUET"]}

all_file_types = {**csv_file_types, **json_file_types, **parquet_file_types}

test_file_types = [
    [key, value]
    for key, list_of_values in all_file_types.items()
    for value in list_of_values
]
# Output:
# [['FileFormat.CSV', 'csv'],
#  ['FileFormat.CSV', 'csv.gzip'],
#  ...
#  ['FileFormat.PARQUET', 'SNAPPY.PARQUET']]

engine_file_types = {
    "pandas": [FileFormat.CSV, FileFormat.JSON],
    "arrow": [FileFormat.CSV, FileFormat.PARQUET],
}
reader_engine_file_types = engine_file_types
writer_engine_file_types = engine_file_types


valid_engines = ["pandas", "arrow", None]
valid_reader_engines = valid_engines
valid_writer_engines = valid_engines


@pytest.fixture
def test_meta():
    return {
        "columns": [
            {"name": "my_float", "type": "float64", "type_category": "float"},
            {"name": "my_bool", "type": "bool_", "type_category": "boolean"},
            {"name": "my_nullable_bool", "type": "bool_", "type_category": "boolean"},
            {"name": "my_date", "type": "date32", "type_category": "timestamp"},
            {
                "name": "my_datetime",
                "type": "timestamp(s)",
                "type_category": "timestamp",
            },
            {"name": "my_int", "type": "int64", "type_category": "integer"},
            {"name": "my_string", "type": "string", "type_category": "string"},
        ]
    }


@pytest.fixture
def df_all_types_from_meta(test_meta):
    return reader.csv.read("tests/data/all_types.csv", test_meta)


@pytest.mark.parametrize("trip1_file_format", all_formats)
@pytest.mark.parametrize("trip2_file_format", all_formats)
def test_round_trip(
    trip1_file_format, trip2_file_format, test_meta, df_all_types_from_meta
):
    original = df_all_types_from_meta
    orig_copy = original.copy()

    # Trip 1
    with tempfile.NamedTemporaryFile() as f:
        tmp_out_file1 = f.name
    writer.write(
        df=orig_copy,
        output_path=tmp_out_file1,
        file_format=trip1_file_format,
        metadata=test_meta,
    )
    df_mid = reader.read(
        input_path=tmp_out_file1, file_format=trip1_file_format, metadata=test_meta
    )

    # Trip 2
    with tempfile.NamedTemporaryFile() as f:
        tmp_out_file2 = f.name
    writer.write(
        df=df_mid,
        output_path=tmp_out_file2,
        file_format=trip2_file_format,
        metadata=test_meta,
    )
    final = reader.read(
        tmp_out_file2, file_format=trip2_file_format, metadata=test_meta
    )

    assert_frame_equal(original, final)


@pytest.mark.parametrize("trip1_file_format", all_formats)
@pytest.mark.parametrize("trip2_file_format", all_formats)
def test_round_trip_chunked(
    trip1_file_format, trip2_file_format, test_meta, df_all_types_from_meta
):
    original = df_all_types_from_meta
    orig_copy = original.copy()

    # Trip 1
    with tempfile.NamedTemporaryFile() as f:
        tmp_out_file1 = f.name
    writer.write(
        orig_copy, tmp_out_file1, file_format=trip1_file_format, metadata=test_meta
    )
    df_mid = reader.read(
        tmp_out_file1, file_format=trip1_file_format, metadata=test_meta, chunksize=2
    )

    # Trip 2
    with tempfile.NamedTemporaryFile() as f:
        tmp_out_file2 = f.name
    writer.write(
        df_mid, tmp_out_file2, file_format=trip2_file_format, metadata=test_meta
    )
    final = reader.read(
        tmp_out_file2, file_format=trip2_file_format, metadata=test_meta
    )

    assert_frame_equal(original, final)


@pytest.mark.parametrize("trip_file_format, trip_file_suffix", test_file_types)
@pytest.mark.parametrize("trip_writer_engine", valid_writer_engines)
def test_round_trip_writer_engines_default_reader(
    trip_file_format,
    trip_file_suffix,
    trip_writer_engine,
    test_meta,
    df_all_types_from_meta,
):
    if trip_writer_engine is not None:
        if trip_file_format not in writer_engine_file_types[trip_writer_engine]:
            pytest.skip(
                "file_format ({trip_file_suffix}) and engine combination is not yet implemented"  # noqa: E501
            )

    original = df_all_types_from_meta
    orig_copy = original.copy()

    with tempfile.NamedTemporaryFile(suffix="." + trip_file_suffix) as temp_file_name:
        temp_out_file = temp_file_name.name

    writer.write(
        df=orig_copy,
        output_path=temp_out_file,
        file_format=trip_file_format,
        metadata=test_meta,
        writer_engine=trip_writer_engine,
    )

    final = reader.read(input_path=temp_out_file, metadata=test_meta)

    assert_frame_equal(original, final)


@pytest.mark.parametrize("trip_file_format, trip_file_suffix", test_file_types)
@pytest.mark.parametrize("trip_reader_engine", valid_reader_engines)
def test_round_trip_reader_engines_default_writer(
    trip_file_format,
    trip_file_suffix,
    trip_reader_engine,
    test_meta,
    df_all_types_from_meta,
):
    if trip_reader_engine is not None:
        if trip_file_format not in reader_engine_file_types[trip_reader_engine]:
            pytest.skip(
                "file_format ({trip_file_suffix}) and engine combination is not yet implemented"  # noqa: E501
            )

    # Default csv reader
    original = df_all_types_from_meta
    orig_copy = original.copy()

    with tempfile.NamedTemporaryFile(suffix="." + trip_file_suffix) as temp_file_name:
        temp_out_file = temp_file_name.name

    # Default writer
    writer.write(
        df=orig_copy,
        output_path=temp_out_file,
        file_format=trip_file_format,
        metadata=test_meta,
    )

    final = reader.read(
        input_path=temp_out_file, metadata=test_meta, reader_engine=trip_reader_engine
    )

    assert_frame_equal(original, final)
