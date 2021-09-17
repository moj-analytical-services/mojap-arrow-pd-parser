import pytest
import tempfile

from pandas.testing import assert_frame_equal

from arrow_pd_parser import reader, writer
from arrow_pd_parser.utils import FileFormat


all_formats = [
    FileFormat.CSV, FileFormat.JSON, FileFormat.PARQUET
]

@pytest.mark.parametrize("trip1_file_format", all_formats)
@pytest.mark.parametrize("trip2_file_format", all_formats)
def test_round_trip(trip1_file_format, trip2_file_format):
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
    original = reader.csv.read("tests/data/all_types.csv", meta)
    orig_copy = original.copy()

    # Trip 1
    with tempfile.NamedTemporaryFile() as f:
        tmp_out_file1 = f.name
    writer.write(orig_copy, tmp_out_file1, file_format=trip1_file_format, metadata=meta)
    df_mid = reader.read(tmp_out_file1, file_format=trip1_file_format, metadata=meta)

    # Trip 2
    with tempfile.NamedTemporaryFile() as f:
        tmp_out_file2 = f.name
    writer.write(df_mid, tmp_out_file2, file_format=trip2_file_format, metadata=meta)
    final = reader.read(tmp_out_file2, file_format=trip2_file_format, metadata=meta)

    assert_frame_equal(original, final)
