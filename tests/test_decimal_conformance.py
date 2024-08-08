import pytest
import pyarrow as pa
from pandas.testing import assert_frame_equal
from arrow_pd_parser._arrow_parsers import pa_read_csv_to_pandas, pa_read_json_to_pandas


@pytest.mark.parametrize(
    "arrow_type,pd_type",
    [("float32", "float32"), ("float64", "float64"), ("decimal", "object")],
)
def test_decimal_float(arrow_type, pd_type):
    type_lu = {
        "float32": pa.float32(),
        "float64": pa.float64(),
        "decimal": pa.decimal128(5, 3),
    }

    schema = pa.schema([("i", pa.int8()), ("my_decimal", type_lu[arrow_type])])

    df_csv = pa_read_csv_to_pandas("tests/data/decimal_type.csv", schema)
    df_json = pa_read_json_to_pandas("tests/data/decimal_type.jsonl", schema)

    assert str(df_csv.my_decimal.dtype) == pd_type
    assert str(df_json.my_decimal.dtype) == pd_type

    assert_frame_equal(df_csv, df_json)
