import pytest
import pyarrow as pa

from arrow_pd_parser.parse_csv import pa_read_csv_to_pandas

@pytest.mark.parametrize("in_type,pd_old_type,pd_new_type",
    [
        ("int8", "float64", "Int64"),
        ("int16", "float64", "Int64"),
        ("int32", "float64", "Int64"),
        ("int64", "float64", "Int64"),
        ("uint8", "float64", "Int64"),
        ("uint16", "float64", "Int64"),
        ("uint32", "float64", "Int64"),
        ("uint64", "float64", "Int64")
    ]
)
def test_int(in_type, pd_old_type, pd_new_type):
    test_col_types = {
        "int_col": getattr(pa, in_type)()
    }
    df_old = pa_read_csv_to_pandas("tests/data/int_type.csv", test_col_types, new_int_type=False)
    assert str(df_old.my_int.dtype) == pd_old_type

    df_new = pa_read_csv_to_pandas("tests/data/int_type.csv", test_col_types, new_int_type=True)
    assert str(df_new.my_int.dtype) == pd_new_type
