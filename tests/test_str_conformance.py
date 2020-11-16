import pytest
import pyarrow as pa
from pyarrow import csv
import pandas as pd

from arrow_pd_parser.parse import pa_read_csv_to_pandas


@pytest.mark.parametrize(
    "in_type,pd_old_type,pd_new_type",
    [("string", "object", "string")],
)
def test_string(in_type, pd_old_type, pd_new_type):
    test_col_types = {"string_col": getattr(pa, "string")()}
    df_old = pa_read_csv_to_pandas(
        "tests/data/string_type.csv", test_col_types, pd_string=False
    )
    assert str(df_old.my_string.dtype) == pd_old_type

    df_new = pa_read_csv_to_pandas(
        "tests/data/string_type.csv", test_col_types, pd_string=True
    )
    assert str(df_new.my_string.dtype) == pd_new_type