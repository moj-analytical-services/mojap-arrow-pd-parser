import pytest
import tempfile

from arrow_pd_parser import reader, writer
from pandas.testing import assert_frame_equal


@pytest.mark.parametrize("data_format", ["jsonl", "csv"])
@pytest.mark.parametrize("use_meta", [True, False])
def test_read(data_format, use_meta):
    test_data_path = f"tests/data/all_types.{data_format}"

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

    df1 = reader.read(test_data_path, meta)

    if data_format == "csv":
        df2 = reader.csv.read(test_data_path, meta)
    elif data_format == "jsonl":
        df2 = reader.json.read(test_data_path, meta)
    else:
        raise ValueError(f"Test wasn't expecting: {data_format}")

    assert_frame_equal(df1, df2)


def test_round_trip():
    meta = {
        "columns": [
            {"name": "my_float", "type": "float64", "type_category": "float"},
            {"name": "my_bool", "type": "bool_", "type_category": "boolean"},
            {"name": "my_nullable_bool", "type": "bool_", "type_category": "boolean"},
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

    # Create parquet temp file
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        tmp_out_file = f.name
    original = reader.csv.read("tests/data/all_types.csv", meta)
    writer.parquet.write(original, tmp_out_file)

    data_paths = {
        "csv": "tests/data/all_types.csv",
        "json": "tests/data/all_types.jsonl",
        "parquet": tmp_out_file,
    }

    for type1 in ["csv", "json", "parquet"]:
        for type2 in ["csv", "json", "parquet"]:
            df1 = reader.read(
                input_path=data_paths[type1],
                metadata=meta,
            )
            df2 = reader.read(
                input_path=data_paths[type2],
                metadata=meta,
            )
            assert_frame_equal(df1, df2)
