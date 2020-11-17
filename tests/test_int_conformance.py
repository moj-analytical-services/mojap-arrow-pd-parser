import pytest
import pyarrow as pa

from arrow_pd_parser.parse import pa_read_csv_to_pandas, pa_read_json_to_pandas


# Parameters used across tests for both json and csv
parameters = [
    ("int8", "float64", "Int64"),
    ("int16", "float64", "Int64"),
    ("int32", "float64", "Int64"),
    ("int64", "float64", "Int64"),
    ("uint8", "float64", "Int64"),
    ("uint16", "float64", "Int64"),
    ("uint32", "float64", "Int64"),
    ("uint64", "float64", "Int64"),
]


@pytest.mark.parametrize("in_type,pd_old_type,pd_new_type", parameters)
def test_int_csv(in_type, pd_old_type, pd_new_type):
    test_col_types = {"int_col": getattr(pa, in_type)()}
    test_file = "tests/data/int_type.csv"

    df_old = pa_read_csv_to_pandas(test_file, test_col_types, pd_integer=False)
    assert str(df_old.my_int.dtype) == pd_old_type

    df_new = pa_read_csv_to_pandas(test_file, test_col_types, pd_integer=True)
    assert str(df_new.my_int.dtype) == pd_new_type


@pytest.mark.parametrize("in_type,pd_old_type,pd_new_type", parameters)
def test_int_jsonl(in_type, pd_old_type, pd_new_type):
    test_col_types = {"int_col": getattr(pa, in_type)()}
    test_file = "tests/data/int_type.jsonl"

    df_old = pa_read_json_to_pandas(test_file, test_col_types, pd_integer=False)
    assert str(df_old.my_int.dtype) == pd_old_type

    df_new = pa_read_json_to_pandas(test_file, test_col_types, pd_integer=True)
    assert str(df_new.my_int.dtype) == pd_new_type
