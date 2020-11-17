import pyarrow as pa
from pandas import DataFrame
from arrow_pd_parser.parse import pa_read_csv_to_pandas, pa_read_json_to_pandas


def test_file_reader():
    assert isinstance(
        pa_read_csv_to_pandas(
            "tests/data/example_data.csv",
            test_col_types={"string_col": getattr(pa, "string")()},
        ),
        DataFrame,
    )
    assert isinstance(pa_read_json_to_pandas("tests/data/example_data.json"), DataFrame)
