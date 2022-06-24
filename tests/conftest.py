import pandas as pd
import pytest
from arrow_pd_parser import reader


@pytest.fixture
def df_all_types():
    return pd.read_csv("tests/data/all_types.csv")


@pytest.fixture
def df_all_types_from_meta(test_meta):
    return reader.csv.read("tests/data/all_types.csv", test_meta)


@pytest.fixture
def test_meta():
    return {
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
