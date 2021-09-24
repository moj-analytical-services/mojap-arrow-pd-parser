import io
import tempfile
import pytest
import pyarrow as pa

from arrow_pd_parser._arrow_parsers import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
    pa_read_parquet_to_pandas,
)
from arrow_pd_parser._export import pd_to_csv, pd_to_json, pd_to_parquet
from pandas.testing import assert_frame_equal

schemas = [
    pa.schema(
        [
            ("i", pa.int8()),
            ("my_float", pa.float64()),
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
            ("my_float", pa.float64()),
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
            ("my_float", pa.float64()),
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
            ("my_float", pa.float64()),
            ("my_bool", pa.bool_()),
            ("my_nullable_bool", pa.bool_()),
            ("my_date", pa.date64()),
            ("my_datetime", pa.timestamp("ns")),
            ("my_int", pa.uint64()),
            ("my_string", pa.string()),
        ]
    ),
]

date_types = ["pd_timestamp", "datetime_object", "pd_period"]


@pytest.mark.parametrize("boolean_args", [True, False])
@pytest.mark.parametrize("date_args", date_types)
@pytest.mark.parametrize("schema", schemas)
def test_pd_to_csv(boolean_args, date_args, schema):
    original = pa_read_csv_to_pandas(
        "tests/data/all_types.csv",
        schema,
        pd_boolean=boolean_args,
        pd_integer=boolean_args,
        pd_string=boolean_args,
        pd_date_type=date_args,
        pd_timestamp_type=date_args,
    )
    # Write to StringIO then convert to BytesIO so Arrow can read it
    output = io.StringIO()
    pd_to_csv(original, output)
    as_bytes = io.BytesIO(bytearray(output.getvalue(), "utf-8"))
    reloaded = pa_read_csv_to_pandas(
        as_bytes,
        schema,
        pd_boolean=boolean_args,
        pd_integer=boolean_args,
        pd_string=boolean_args,
        pd_date_type=date_args,
        pd_timestamp_type=date_args,
    )
    assert_frame_equal(original, reloaded)


@pytest.mark.parametrize("boolean_args", [True, False])
@pytest.mark.parametrize("date_args", date_types)
@pytest.mark.parametrize("schema", schemas)
def test_pd_to_json(boolean_args, date_args, schema):
    original = pa_read_csv_to_pandas(
        "tests/data/all_types.csv",
        schema,
        pd_boolean=boolean_args,
        pd_integer=boolean_args,
        pd_string=boolean_args,
        pd_date_type=date_args,
        pd_timestamp_type=date_args,
    )
    # Write to StringIO then convert to BytesIO so Arrow can read it
    output = io.StringIO()
    pd_to_json(original, output)
    as_bytes = io.BytesIO(bytearray(output.getvalue(), "utf-8"))
    reloaded = pa_read_json_to_pandas(
        as_bytes,
        schema,
        pd_boolean=boolean_args,
        pd_integer=boolean_args,
        pd_string=boolean_args,
        pd_date_type=date_args,
        pd_timestamp_type=date_args,
    )
    assert_frame_equal(original, reloaded)


@pytest.mark.parametrize("boolean_args", [True, False])
@pytest.mark.parametrize("date_args", date_types)
@pytest.mark.parametrize("schema", schemas)
def test_to_parquet(schema, boolean_args, date_args):
    original = pa_read_csv_to_pandas(
        "tests/data/all_types.csv",
        schema,
        pd_boolean=boolean_args,
        pd_integer=boolean_args,
        pd_string=boolean_args,
        pd_date_type=date_args,
        pd_timestamp_type=date_args,
    )

    # output as parquet
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        tmp_out_file = f.name
    pd_to_parquet(original, tmp_out_file)

    # read in as parquet
    reloaded = pa_read_parquet_to_pandas(
        tmp_out_file,
        schema,
        pd_boolean=boolean_args,
        pd_integer=boolean_args,
        pd_string=boolean_args,
        pd_date_type=date_args,
        pd_timestamp_type=date_args,
    )

    assert_frame_equal(original, reloaded)
