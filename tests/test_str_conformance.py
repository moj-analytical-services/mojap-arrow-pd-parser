import pytest
import pyarrow as pa

from arrow_pd_parser.parse import pa_read_csv_to_pandas


@pytest.mark.parametrize(
    "in_type,pd_old_type,pd_new_type", [("string", "object", "string")],
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


@pytest.mark.parametrize(
    "in_type,pd_old_type,pd_new_type", [("string", "object", "string")],
)
def test_csv_options(in_type, pd_old_type, pd_new_type):
    test_col_types = {"string_col": getattr(pa, "string")()}
    parse_options = {
        "quote_char": "'",
        "escape_char": "\\",
        "delimiter": ";",
        "newlines_in_values": True,
    }
    read_options = {"skip_rows": 1}
    convert_options = {
        "include_columns": ["i", "my_string", "nonexistent_column"],
        "include_missing_columns": True,
        "null_values": ["NULL_STRING"],
    }

    df = pa_read_csv_to_pandas(
        "tests/data/csv_options_test.csv",
        test_col_types,
        pd_string=False,
        parse_options=parse_options,
        convert_options=convert_options,
        read_options=read_options,
    )
    print(df.columns.tolist())
    assert df.columns.tolist() == ["i", "my_string", "nonexistent_column"]
