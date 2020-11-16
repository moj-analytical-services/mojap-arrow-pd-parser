import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import csv


def pa_read_csv(csv_path, test_col_types):
    csv_co = csv.ConvertOptions(column_types=test_col_types)
    pa_csv_table = csv.read_csv(csv_path, convert_options=csv_co)
    return pa_csv_table


def pa_to_pd(arrow_table, pd_integer: bool = True):
    if pd_integer:
        int_mapper = {
            pa.int8(): pd.Int64Dtype(),
            pa.int16(): pd.Int64Dtype(),
            pa.int32(): pd.Int64Dtype(),
            pa.int64(): pd.Int64Dtype(),
            pa.uint8(): pd.Int64Dtype(),
            pa.uint16(): pd.Int64Dtype(),
            pa.uint32(): pd.Int64Dtype(),
            pa.uint64(): pd.Int64Dtype(),
        }
        return arrow_table.to_pandas(
            timestamp_as_object=True, types_mapper=int_mapper.get
        )

    else:
        int_mapper = {
            pa.int8: np.float64,
            pa.int16: np.float64,
            pa.int32: np.float64,
            pa.int64: np.float64,
            pa.uint8: np.float64,
            pa.uint16: np.float64,
            pa.uint32: np.float64,
            pa.uint64: np.float64,
        }
        return arrow_table.to_pandas(
            timestamp_as_object=True, types_mapper=int_mapper.get
        )


def pa_read_csv_to_pandas(csv_path, test_col_types, pd_integer: bool = True):
    arrow_table = pa_read_csv(csv_path, test_col_types)
    return pa_to_pd(arrow_table, pd_integer=pd_integer)
