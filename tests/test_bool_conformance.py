import pytest
import pyarrow as pa

from arrow_pd_parser.parse import pa_read_csv_to_pandas, pa_read_json_to_pandas


args = (
    ("col_name", "pd_old_type", "pd_new_type"),
    [("my_bool", "bool", "boolean"), ("my_nullable_bool", "object", "boolean")],
)


@pytest.mark.parametrize(*args)
def test_bool_csv(col_name, pd_old_type, pd_new_type):

    schema = pa.schema([("bool_col", pa.bool_())])
    df_old = pa_read_csv_to_pandas("tests/data/bool_type.csv", schema, pd_boolean=False)
    assert str(df_old[col_name].dtype) == pd_old_type

    df_new = pa_read_csv_to_pandas("tests/data/bool_type.csv", schema, pd_boolean=True)
    assert str(df_new[col_name].dtype) == pd_new_type


@pytest.mark.parametrize(*args)
def test_bool_jsonl(col_name, pd_old_type, pd_new_type):
    df_old = pa_read_json_to_pandas("tests/data/bool_type.jsonl", pd_boolean=False)
    assert str(df_old[col_name].dtype) == pd_old_type

    df_new = pa_read_json_to_pandas("tests/data/bool_type.jsonl", pd_boolean=True)
    assert str(df_new[col_name].dtype) == pd_new_type


def test_bool_csv_and_json():
    schema = pa.schema([("bool_col", pa.bool_())])
    df_csv = pa_read_csv_to_pandas("tests/data/bool_type.csv", schema, pd_boolean=True)
    df_jsonl = pa_read_json_to_pandas("tests/data/bool_type.jsonl", pd_boolean=True)
    assert df_csv.equals(df_jsonl)
