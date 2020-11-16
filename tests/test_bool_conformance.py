import pytest
import pyarrow as pa

from arrow_pd_parser.parse_csv import pa_read_csv_to_pandas


@pytest.mark.parametrize(
    "col_name,pd_old_type,pd_new_type",
    [("my_bool", "bool", "boolean"), ("my_nullable_bool", "object", "boolean")],
)
def test_bool(col_name, pd_old_type, pd_new_type):
    test_col_types = {"bool_col": getattr(pa, "bool_")()}
    df_old = pa_read_csv_to_pandas(
        "tests/data/bool_type.csv", test_col_types, new_bool_type=False
    )
    assert str(df_old[col_name].dtype) == pd_old_type

    df_new = pa_read_csv_to_pandas(
        "tests/data/bool_type.csv", test_col_types, new_bool_type=True
    )
    assert str(df_new[col_name].dtype) == pd_new_type
