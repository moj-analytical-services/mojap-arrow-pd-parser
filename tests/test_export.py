import os
import pyarrow as pa

from arrow_pd_parser.parse import pa_read_csv_to_pandas, pa_read_json_to_pandas
from arrow_pd_parser.export import pd_to_csv, pd_to_json
from pandas.testing import assert_frame_equal

schema = pa.schema(
    [
        ("i", pa.int64()),
        ("my_bool", pa.bool_()),
        ("my_nullable_bool", pa.bool_()),
        ("my_date", pa.date64()),
        ("my_datetime", pa.timestamp("s")),
        ("my_int", pa.int64()),
        ("my_string", pa.string()),
    ]
)


def test_pd_to_csv():
    #original = pa_read_csv_to_pandas("tests/data/all_types.csv", schema)
    original = pa_read_json_to_pandas("tests/data/all_types.jsonl", schema)

    try:
        pd_to_csv(original, "tests/data/export_test.csv")
        reloaded = pa_read_csv_to_pandas("tests/data/export_test.csv", schema)
        assert_frame_equal(original, reloaded)

    finally:
        os.remove("tests/data/export_test.csv")


def test_pd_to_json():
    #original = pa_read_csv_to_pandas("tests/data/all_types.csv", schema)
    original = pa_read_json_to_pandas("tests/data/all_types.jsonl", schema)

    try:
        pd_to_json(original, "tests/data/export_test2.jsonl")
        reloaded = pa_read_json_to_pandas("tests/data/export_test2.jsonl", schema)
        assert_frame_equal(original, reloaded)

    finally:
        #os.remove("tests/data/export_test.json")
        pass
