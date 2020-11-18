import pyarrow as pa
from pandas import DataFrame

from pandas.testing import assert_frame_equal
from arrow_pd_parser.parse import pa_read_csv_to_pandas, pa_read_json_to_pandas

def test_file_reader():

    csv_schema = pa.schema([
        ("test", pa.string()),
        ("a_column", pa.string())
    ])
    df_csv = pa_read_csv_to_pandas("tests/data/example_data.csv")
    df_csv_schema = pa_read_csv_to_pandas(
        "tests/data/example_data.csv",
        csv_schema
    )
    assert_frame_equal(df_csv, df_csv_schema)

    json_schema = pa.schema([
        ("a", pa.int64()),
        ("b", pa.float64()),
        ("c", pa.string()),
        ("d", pa.bool_()),
    ])
    df_json = pa_read_json_to_pandas("tests/data/example_data.jsonl")
    df_json_schema = pa_read_json_to_pandas(
        "tests/data/example_data.jsonl",
        json_schema
    )
    assert_frame_equal(df_json, df_json_schema)
