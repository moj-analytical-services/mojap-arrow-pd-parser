import pytest

from io import StringIO
from datetime import datetime

import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal, assert_series_equal
from arrow_pd_parser.parse import (
    pd_read_csv,
    pd_read_json,
    cast_pandas_table_to_schema,
    cast_pandas_column_to_schema,
    PandasCastError,
)

from arrow_pd_parser.parse.pandas_parser import (
    _infer_bool_type,
    convert_to_bool_series,
    convert_str_to_timestamp_series,
)

from jsonschema.exceptions import ValidationError

from mojap_metadata import Metadata


def test_file_reader_returns_df():
    csv_meta = {
        "columns": [
            {"name": "test", "type_category": "string"},
            {"name": "a_column", "type_category": "string"},
        ]
    }

    df = pd_read_csv("tests/data/example_data.csv", metadata=csv_meta)
    assert isinstance(df, pd.DataFrame)
    assert [str(df[c].dtype) for c in df.columns] == ["string", "string"]

    # df = pa_read_json("tests/data/example_data.jsonl")
    # assert isinstance(df, pd.DataFrame)


def test_file_reader_works_with_both_meta_types():
    csv_meta = {
        "columns": [
            {"name": "test", "type_category": "string"},
            {"name": "a_column", "type_category": "string"},
        ]
    }
    df_csv1 = pd_read_csv("tests/data/example_data.csv", csv_meta)
    csv_meta = Metadata.from_dict(csv_meta)
    df_csv2 = pd_read_csv("tests/data/example_data.csv", csv_meta)
    assert_frame_equal(df_csv1, df_csv2)
    with pytest.raises(KeyError):
        pd_read_csv("tests/data/example_data.jsonl", metadata={})
    with pytest.raises(ValidationError):
        m = {"columns": [{"name": "broken", "type": "not-a-type"}]}
        pd_read_csv("tests/data/example_data.jsonl", metadata=m)


@pytest.mark.parametrize("data_format", ["jsonl", "csv"])
def test_basic_end_to_end(data_format):

    test_data_path = f"tests/data/all_types.{data_format}"

    meta = {
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

    if data_format == "jsonl":
        df = pd.read_json(test_data_path, lines=True)
    else:
        df = pd.read_csv(test_data_path, dtype="string", low_memory=False)

    dfn = cast_pandas_table_to_schema(df, meta)

    expected_dtypes = {
        "my_float": "float64",
        "my_bool": "boolean",
        "my_nullable_bool": "boolean",
        "my_date": "object",
        "my_datetime": "object",
        "my_int": "Int64",
        "my_string": "string",
    }

    actual_dtypes = {}
    for c in dfn.columns:
        actual_dtypes[c] = str(dfn[c].dtype)
    assert actual_dtypes == expected_dtypes

    if data_format == "jsonl":
        df2 = pd_read_json(test_data_path, meta)
        with pytest.warns(UserWarning):
            df3 = pd_read_json(test_data_path, meta, orient="records", lines=True)
    else:
        df2 = pd_read_csv(test_data_path, meta)
        df3 = pd_read_csv(test_data_path, meta, dtype=str, low_memory=False)

    assert_frame_equal(dfn, df2)
    assert_frame_equal(df2, df3)


@pytest.mark.parametrize(
    "s,expected_category,bool_map",
    [
        (pd.Series([True, False, True], dtype=bool), "bool", None),
        (pd.Series([True, False, None], dtype=object), "bool_object", None),
        (pd.Series([True, False, pd.NA], dtype=pd.BooleanDtype()), "boolean", None),
        (pd.Series(["True", "False", np.nan], dtype=str), "str_object", None),
        (
            pd.Series(["True", "False", None], dtype=pd.StringDtype()),
            "str_object",
            None,
        ),
        (pd.Series(["T", "F", np.nan], dtype=str), "str_object", None),
        (pd.Series(["1.0", "0.0", np.nan], dtype=str), "str_object", None),
        (
            pd.Series(["Yes", "No", np.nan], dtype=str),
            "str_object",
            {"Yes": True, "No": False},
        ),
        (pd.Series([1, 0, 1], dtype=int), "numeric", None),
        (pd.Series([1, 0, np.nan], dtype=float), "numeric", None),
        (pd.Series([1.0, 0.0, np.nan], dtype=float), "numeric", None),
    ],
)
def test_boolean_conversion(s, expected_category, bool_map):
    assert _infer_bool_type(s) == expected_category

    if pd.isna(s[2]):
        expected = pd.Series([True, False, pd.NA], dtype=pd.BooleanDtype())
    else:
        expected = pd.Series([True, False, True], dtype=pd.BooleanDtype())
    actual = convert_to_bool_series(s, True, bool_map=bool_map)
    assert_series_equal(expected, actual)


@pytest.mark.parametrize(
    "s,dt_fmt,is_date",
    [
        (pd.Series(["1970-01-01", "2021-12-31", None], dtype=str), None, True),
        (pd.Series(["1970-01-01", "2021-12-31", None], dtype=str), "%Y-%m-%d", True),
        (pd.Series(["01-Jan-70", "31-Dec-21", None], dtype=str), "%d-%b-%y", True),
        (pd.Series(["01-Jan-70", "31-Dec-21", None], dtype=str), "%d-%b-%y", False),
        (
            pd.Series(["1970-01-01 00:00:00", "2021-12-31 23:59:59", None], dtype=str),
            None,
            False,
        ),
        (
            pd.Series(["1970-01-01 00:00:00", "2021-12-31 23:59:59", None], dtype=str),
            "%Y-%m-%d %H:%M:%S",
            False,
        ),
    ],
)
@pytest.mark.parametrize(
    "pd_date_type", ["datetime_object", "pd_timestamp", "pd_period"]
)
def test_timestamp_conversion(s, dt_fmt, is_date, pd_date_type):
    if pd_date_type == "pd_period":
        with pytest.raises(NotImplementedError):
            s_ = convert_str_to_timestamp_series(s, is_date, pd_date_type, dt_fmt)
    else:
        s_ = convert_str_to_timestamp_series(s, is_date, pd_date_type, dt_fmt)
        assert_series_equal(pd.to_datetime(s, format=dt_fmt), pd.to_datetime(s_))


@pytest.mark.parametrize("col_type", ["date64", "date32", "timestamp(s)"])
def test_timestamp_conversion_in_df(col_type):
    meta = {
        "name": "test",
        "columns": [
            {"name": "datelong", "datetime_format": "%d-%b-%Y"},
            {"name": "dateshort", "datetime_format": "%d-%b-%y"},
            {"name": "date_uk", "datetime_format": "%d/%m/%Y"},
        ],
    }
    for c in meta["columns"]:
        c["type"] = col_type
        c["type_category"] = "timestamp"

    data = (
        "datelong,dateshort,date_uk\n"
        "01-JAN-2020,01-JAN-20,01/01/2020\n"
        "27-MAY-1996,27-MAY-96,27/05/1996\n"
    )
    expected_col_values = [datetime(2020, 1, 1), datetime(1996, 5, 27)]
    if col_type.startswith("date"):
        expected_col_values = [v.date() for v in expected_col_values]

    df = pd_read_csv(StringIO(data), metadata=meta)
    for c in df.columns:
        assert expected_col_values == df[c].to_list()


def test_cast_error():
    col = pd.Series(["1970-01-01", "2021-12-31", None], dtype=str)
    mc = {
        "name": "bad_format",
        "type": "date64()",
        "type_category": "timestamp",
        "datetime_format": "%d/%m/%Y %H:%M:%S",
    }
    with pytest.raises(PandasCastError) as exec_info:
        cast_pandas_column_to_schema(col, mc)

    failed_msg = (
        "Failed conversion - name: bad_format | "
        "type_category: timestamp | type: date64() - see traceback."
    )
    assert str(exec_info.value).startswith(failed_msg)


@pytest.mark.parametrize(
    "row,t,tc",
    [
        ({"a": 0, "b": "string"}, "struct<a:int64, b:string>", "struct"),
        ([0, 1, 2], "list<int64>", "list"),
        ([0, 1, 2], "large_list<int64>", "list"),
    ],
)
def test_complex_cast_warning(row, t, tc):
    col = pd.Series({"complex_col": [row, row]})
    mc = {"name": "complex_col", "type": t, "type_category": tc}
    with pytest.warns(UserWarning):
        cast_pandas_column_to_schema(col, mc)
