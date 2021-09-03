import pytest
import pyarrow as pa
from pyarrow import csv
from arrow_pd_parser.parse import pa_read_csv_to_pandas
from pandas.testing import assert_series_equal
from pandas import Series


@pytest.mark.parametrize(
    "in_type,pd_old_type,pd_new_type",
    [("string", "object", "string")],
)
def test_string(in_type, pd_old_type, pd_new_type):

    schema = pa.schema([("string_col", pa.string())])
    df_old = pa_read_csv_to_pandas(
        "tests/data/string_type.csv", schema, False, pd_string=False
    )
    assert str(df_old.my_string.dtype) == pd_old_type

    df_new = pa_read_csv_to_pandas(
        "tests/data/string_type.csv", schema, False, pd_string=True
    )
    assert str(df_new.my_string.dtype) == pd_new_type


@pytest.mark.parametrize(
    "in_type,pd_old_type,pd_new_type",
    [("string", "object", "string")],
)
def test_csv_options(in_type, pd_old_type, pd_new_type):
    schema = pa.schema([("string_col", pa.string())])

    read_options = csv.ReadOptions(skip_rows=1)

    parse_options = csv.ParseOptions(
        quote_char="'", escape_char="\\", delimiter=";", newlines_in_values=True
    )

    convert_options = csv.ConvertOptions(
        include_columns=["i", "my_string", "nonexistent_column"],
        include_missing_columns=True,
        null_values=["NULL_STRING"],
        strings_can_be_null=True,
    )

    df = pa_read_csv_to_pandas(
        "tests/data/csv_options_test.csv",
        schema,
        False,
        pd_string=False,
        parse_options=parse_options,
        convert_options=convert_options,
        read_options=read_options,
    )

    expected = [
        "dsfasd;dsffadsf",
        "dsfasd;dsffadsf",
        None,
        "this text\nhas a line break",
        "this text, like so, has commas",
    ]
    assert df.columns.tolist() == ["i", "my_string", "nonexistent_column"]
    assert df["nonexistent_column"].isnull().all()
    assert_series_equal(df["my_string"], Series(expected, name="my_string"))
