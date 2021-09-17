import pytest
import tempfile

from arrow_pd_parser import writer, reader
from pandas.testing import assert_frame_equal
from arrow_pd_parser._export import pd_to_parquet


@pytest.mark.parametrize("data_format", ["jsonl", "csv", "snappy.parquet"])
@pytest.mark.parametrize("use_meta", [True, False])
def test_write(data_format, use_meta):

    if use_meta:
        meta = {
            "columns": [
                {"name": "my_float", "type": "float64", "type_category": "float"},
                {"name": "my_bool", "type": "bool_", "type_category": "boolean"},
                {
                    "name": "my_nullable_bool",
                    "type": "bool_",
                    "type_category": "boolean",
                },
                {"name": "my_date", "type": "date32", "type_category": "timestamp"},
                {
                    "name": "my_datetime",
                    "type": "timestamp(s)",
                    "type_category": "timestamp",
                },
                {"name": "my_int", "type": "int64", "type_category": "integer"},
                {"name": "my_string", "type": "string", "type_category": "string"},
            ]
        }
    else:
        meta = None

    in_data_path = "tests/data/all_types.csv"
    df = reader.read(in_data_path, meta)

    # Create temp files
    with tempfile.NamedTemporaryFile(suffix=f".{data_format}") as f:
        tmp_out1 = f.name
    with tempfile.NamedTemporaryFile(suffix=f".{data_format}") as f:
        tmp_out2 = f.name

    writer.write(df, tmp_out1, meta)
    if data_format == "csv":
        writer.csv.write(df, tmp_out2, meta)
    elif data_format == "jsonl":
        writer.json.write(df, tmp_out2, meta)
    elif data_format == "snappy.parquet":
        writer.parquet.write(df, tmp_out2, meta)
    else:
        raise ValueError(f"Test wasn't expecting: {data_format}")

    with open(tmp_out1, "rb") as f:
        b1 = f.read()
    with open(tmp_out2, "rb") as f:
        b2 = f.read()

    assert b1 == b2
