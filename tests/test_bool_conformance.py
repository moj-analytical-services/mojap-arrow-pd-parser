import pytest
import pyarrow as pa

from arrow_pd_parser.parse import pa_read_csv_to_pandas, pa_read_json_to_pandas


args = (
    ("col_name", "pd_old_type", "pd_new_type"),
    [("my_bool", "bool", "boolean"), ("my_nullable_bool", "object", "boolean")],
)


@pytest.mark.parametrize(
    "my_bool_dtype,my_nullable_bool_dtype,pd_boolean,data_type",
    [
        ("boolean", "boolean", True, "csv"),
        ("bool", "object", False, "csv"),
        ("boolean", "boolean", True, "jsonl"),
        ("bool", "object", False, "jsonl"),
    ],
)
def test_bool(my_bool_dtype, my_nullable_bool_dtype, pd_boolean, data_type):

    schema = pa.schema(
        [("i", pa.int8()), ("my_bool", pa.bool_()), ("my_nullable_bool", pa.bool_())]
    )

    function_lu = {"csv": pa_read_csv_to_pandas, "jsonl": pa_read_json_to_pandas}

    df = function_lu[data_type](
        f"tests/data/bool_type.{data_type}", schema, pd_boolean=pd_boolean
    )

    assert str(df["my_bool"].dtype) == my_bool_dtype
    assert str(df["my_nullable_bool"].dtype) == my_nullable_bool_dtype


def test_bool_csv_and_json():
    schema = pa.schema(
        [("i", pa.int8()), ("my_bool", pa.bool_()), ("my_nullable_bool", pa.bool_())]
    )
    df_csv = pa_read_csv_to_pandas("tests/data/bool_type.csv", schema, pd_boolean=True)
    df_jsonl = pa_read_json_to_pandas(
        "tests/data/bool_type.jsonl", schema, pd_boolean=True
    )
    assert df_csv.equals(df_jsonl)
