import pandas as pd
import pyarrow as pa
from pyarrow import csv


def pa_read_csv(csv_path, test_col_types):
    csv_co = csv.ConvertOptions(column_types=test_col_types)
    pa_csv_table = csv.read_csv(csv_path, convert_options=csv_co)
    return pa_csv_table


def int_mapper(data_type):
    if data_type in [
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
    ]:
        return pd.Int64Dtype()


def pa_to_pd(arrow_table, new_int_type: bool = True):
    if new_int_type:
        return arrow_table.to_pandas(timestamp_as_object=True, types_mapper=int_mapper)
    else:
        return arrow_table.to_pandas(timestamp_as_object=True)


def pa_read_csv_to_pandas(csv_path, test_col_types, new_int_type: bool = True):
    arrow_table = pa_read_csv(csv_path, test_col_types)
    return pa_to_pd(arrow_table, new_int_type=new_int_type)
