import os
import pytest
import pyarrow as pa

from arrow_pd_parser.parse import pa_read_csv_to_pandas, pa_read_json_to_pandas
from arrow_pd_parser.export import pd_to_csv, pd_to_json
from pandas.testing import assert_frame_equal


@pytest.mark.parametrize("boolean_args", [True, False])
@pytest.mark.parametrize("date_args", ["pd_timestamp", "datetime_object", "pd_period"])
@pytest.mark.parametrize(
    "schema",
    [
        pa.schema(
            [
                ("i", pa.int8()),
                ("my_bool", pa.bool_()),
                ("my_nullable_bool", pa.bool_()),
                ("my_date", pa.date32()),
                ("my_datetime", pa.timestamp("s")),
                ("my_int", pa.uint8()),
                ("my_string", pa.string()),
            ]
        ),
        pa.schema(
            [
                ("i", pa.int16()),
                ("my_bool", pa.bool_()),
                ("my_nullable_bool", pa.bool_()),
                ("my_date", pa.date32()),
                ("my_datetime", pa.timestamp("ms")),
                ("my_int", pa.uint16()),
                ("my_string", pa.string()),
            ]
        ),
        pa.schema(
            [
                ("i", pa.int32()),
                ("my_bool", pa.bool_()),
                ("my_nullable_bool", pa.bool_()),
                ("my_date", pa.date64()),
                ("my_datetime", pa.timestamp("us")),
                ("my_int", pa.uint32()),
                ("my_string", pa.string()),
            ]
        ),
        pa.schema(
            [
                ("i", pa.int64()),
                ("my_bool", pa.bool_()),
                ("my_nullable_bool", pa.bool_()),
                ("my_date", pa.date64()),
                ("my_datetime", pa.timestamp("ns")),
                ("my_int", pa.uint64()),
                ("my_string", pa.string()),
            ]
        ),
    ],
)
class TestExport:
    def test_pd_to_csv(self, boolean_args, date_args, schema):
        original = pa_read_csv_to_pandas(
            "tests/data/all_types.csv",
            schema,
            pd_boolean=boolean_args,
            pd_integer=boolean_args,
            pd_string=boolean_args,
            pd_date_type=date_args,
            pd_timestamp_type=date_args,
        )
        try:
            pd_to_csv(original, "tests/data/export_test.csv")
            reloaded = pa_read_csv_to_pandas(
                "tests/data/export_test.csv",
                schema,
                pd_boolean=boolean_args,
                pd_integer=boolean_args,
                pd_string=boolean_args,
                pd_date_type=date_args,
                pd_timestamp_type=date_args,
            )
            assert_frame_equal(original, reloaded)

        finally:
            os.remove("tests/data/export_test.csv")

    def test_pd_to_json(self, boolean_args, date_args, schema):
        original = pa_read_csv_to_pandas(
            "tests/data/all_types.csv",
            schema,
            pd_boolean=boolean_args,
            pd_integer=boolean_args,
            pd_string=boolean_args,
            pd_date_type=date_args,
            pd_timestamp_type=date_args,
        )

        try:
            pd_to_json(original, "tests/data/export_test.jsonl")
            reloaded = pa_read_json_to_pandas(
                "tests/data/export_test.jsonl",
                schema,
                pd_boolean=boolean_args,
                pd_integer=boolean_args,
                pd_string=boolean_args,
                pd_date_type=date_args,
                pd_timestamp_type=date_args,
            )
            assert_frame_equal(original, reloaded)

        finally:
            os.remove("tests/data/export_test.jsonl")
