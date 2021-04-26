import pytest
from io import StringIO

import pyarrow as pa
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal, assert_series_equal
from arrow_pd_parser.parse import (
    pd_read_csv,
    cast_pandas_table_to_schema
)
from arrow_pd_parser.parse.pandas_parser import (
    _infer_bool_type,
    convert_to_bool_series
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


def test_basic_end_to_end():
    data = """
    i,my_float,my_bool,my_nullable_bool,my_date,my_datetime,my_int,my_string
    0,124.1252513,True,True,2013-06-13,2013-06-13 05:11:07,,bhaskjfhsaf
    1,124.1252513,True,True,1995-04-30,1995-04-30 10:23:29,16,💩
    2,0.1252513,False,False,2017-10-15,2017-10-15 20:25:05,16,"dsfasd,dsffadsf"
    3,0.1252513,True,True,1991-12-27,1991-12-27 06:57:23,17,csjasof fweh lkia hfeaofh
    4,0.1252513,True,,1980-03-28,1980-03-28 07:31:18,18,aflhnas flk; h
    5,125195315,True,True,1984-04-21,1984-04-21 18:36:57,,NULL
    6,125195315.0,True,True,,1992-11-08 17:18:12,13,hjsaldfh
    7,1.000001,False,False,1972-10-21,,11,None
    8,,True,True,1973-05-18,1973-05-18 07:35:46,10,null
    9,1.000,True,True,1991-03-13,1991-03-13 15:48:11,11,
    """

    meta = {
        "columns": [
            {"name": "my_float", "type": "float64", "type_category": "float"},
            {"name": "my_bool", "type": "bool_", "type_category": "boolean"},
            {"name": "my_nullable_bool", "type": "bool_", "type_category": "boolean"},
            {"name": "my_date", "type": "date32", "type_category": "timestamp"},
            {"name": "my_datetime", "type": "timestamp(s)", "type_category": "timestamp"},
            {"name": "my_int", "type": "int64", "type_category": "integer"},
            {"name": "my_string", "type": "string", "type_category": "string"},
        ]
    }

    df = pd.read_csv(StringIO(data), dtype="string", low_memory=False)
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

    df2 = pd_read_csv(StringIO(data), meta)
    df3 = pd_read_csv(StringIO(data), meta, dtype=str, low_memory=False)

    assert_frame_equal(dfn, df2)
    assert_frame_equal(df2, df3)


@pytest.mark.parametrize(
    "s,expected_category,bool_map",
    [
        (pd.Series([True, False, True], dtype = bool), "bool", None),
        (pd.Series([True, False, None], dtype = object), "bool_object", None),
        (pd.Series([True, False, pd.NA], dtype = pd.BooleanDtype()), "boolean", None),
        (pd.Series(["True", "False", np.nan], dtype = str),  "str_object", None),
        (pd.Series(["True", "False", None], dtype = pd.StringDtype()),  "str_object", None),
        (pd.Series(["T", "F", np.nan], dtype = str),  "str_object", None),
        (pd.Series(["1.0", "0.0", np.nan], dtype = str),  "str_object", None),
        (pd.Series(["Yes", "No", np.nan], dtype = str),  "str_object", {"Yes": True, "No": False}),
    ]
)
def test_boolean_conversion(s, expected_category, bool_map):
    assert _infer_bool_type(s) == expected_category
    
    if expected_category == "bool":
        expected = pd.Series([True, False, True], dtype = pd.BooleanDtype())
    else:
        expected = pd.Series([True, False, pd.NA], dtype = pd.BooleanDtype())
    actual = convert_to_bool_series(s, True, bool_map=bool_map)
    assert_series_equal(expected, actual)
