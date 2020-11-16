import pandas as pd
import pyarrow as pa
from pyarrow import csv

from arrow_pd_parser.pa_pd import arrow_to_pandas


def pa_read_csv(csv_path, test_col_types):
    csv_co = csv.ConvertOptions(column_types=test_col_types)
    pa_csv_table = csv.read_csv(csv_path, convert_options=csv_co)
    return pa_csv_table


def pa_read_csv_to_pandas(
    csv_path, test_col_types, pd_integer: bool = True, pd_string: bool = True
):
    arrow_table = pa_read_csv(csv_path, test_col_types)
    return arrow_to_pandas(arrow_table, pd_integer=pd_integer, pd_string=pd_string)
