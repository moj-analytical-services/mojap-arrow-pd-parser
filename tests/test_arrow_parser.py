import pytest
import pyarrow as pa
import pandas as pd
from pandas.testing import assert_frame_equal
from arrow_pd_parser._arrow_parsers import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
    cast_arrow_table_to_schema,
    update_existing_schema,
)


def test_file_reader_returns_df():
    df = pa_read_csv_to_pandas("tests/data/example_data.csv")
    assert isinstance(df, pd.DataFrame)

    df = pa_read_json_to_pandas("tests/data/example_data.jsonl")
    assert isinstance(df, pd.DataFrame)


def test_file_reader_works_with_schema():
    csv_schema = pa.schema([("test", pa.string()), ("a_column", pa.string())])
    df_csv = pa_read_csv_to_pandas("tests/data/example_data.csv")
    df_csv_schema = pa_read_csv_to_pandas("tests/data/example_data.csv", csv_schema)
    assert_frame_equal(df_csv, df_csv_schema)

    json_schema = pa.schema(
        [("a", pa.int64()), ("b", pa.float64()), ("c", pa.string()), ("d", pa.bool_())]
    )
    df_json = pa_read_json_to_pandas("tests/data/example_data.jsonl")
    df_json_schema = pa_read_json_to_pandas(
        "tests/data/example_data.jsonl", json_schema
    )
    assert_frame_equal(df_json, df_json_schema)

    # Check raises error on both readers
    missing_schema = pa.schema(
        [("b", pa.float64()), ("c", pa.string()), ("d", pa.bool_())]
    )
    with pytest.raises(ValueError):
        pa_read_json_to_pandas("tests/data/example_data.jsonl", missing_schema)
    with pytest.raises(ValueError):
        pa_read_csv_to_pandas("tests/data/example_data.csv", missing_schema)


def test_update_existing_schema():
    current = pa.schema(
        [("col1", pa.int8()), ("col2", pa.string()), ("col3", pa.decimal128(5, 3))]
    )

    new = pa.schema(
        [("col1", pa.int64()), ("col3", pa.float64()), ("col4", pa.binary())]
    )

    expected = pa.schema(
        [("col1", pa.int64()), ("col2", pa.string()), ("col3", pa.float64())]
    )

    actual = update_existing_schema(current, new)
    assert actual == expected


def test_arrow_table_cast():
    expected_schema = pa.schema([("i", pa.int8()), ("my_int", pa.string())])
    tab = pa.csv.read_csv("tests/data/int_type.csv")
    new_tab1 = cast_arrow_table_to_schema(tab, expected_schema)

    assert new_tab1.schema == expected_schema
    assert new_tab1.schema != tab.schema

    # Check errors
    missing_schema = pa.schema([("my_int", pa.string())])
    with pytest.raises(ValueError):
        _ = cast_arrow_table_to_schema(tab, missing_schema)

    new_tab2 = cast_arrow_table_to_schema(tab, missing_schema, False)

    expected_schema = update_existing_schema(tab.schema, missing_schema)

    assert new_tab2.schema == expected_schema
    assert new_tab2.schema != tab.schema
