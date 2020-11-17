import pytest
import datetime
import pandas as pd
import pyarrow as pa
import numpy as np
from arrow_pd_parser.parse import pa_read_csv_to_pandas

def pd_str_datetime_series_to_list(s):
    str_dates = [None if pd.isna(x) else x for x in s.to_list()]
    return str_dates


def datetime_object_as_str(x):
    if pd.isna(x):
        return np.nan
    else:
        return str(x)


@pytest.mark.parametrize(
    "in_type,as_datetime_object,as_pd_timestamp,as_pd_period",
    [
        ("timestamp[s]", "object", "datetime64[ns]", "period[S]"),
        ("timestamp[ms]", "object", "datetime64[ns]", "period[L]"),
        ("timestamp[us]", "object", "datetime64[ns]", "period[U]"),
        ("timestamp[ns]", "datetime64[ns]", "datetime64[ns]", "period[N]"),
    ],
)
def test_datetime(in_type, as_datetime_object, as_pd_timestamp, as_pd_period):

    test_data_path = "tests/data/datetime_type.csv"

    type_dict = {
        "timestamp[s]": pa.timestamp("s"),
        "timestamp[ms]": pa.timestamp("ms"),
        "timestamp[us]": pa.timestamp("us"),
        "timestamp[ns]": pa.timestamp("ns"),
    }

    test_col_types = {"my_datetime": type_dict[in_type]}

    # datetime_object
    df = pa_read_csv_to_pandas(
        test_data_path,
        test_col_types=test_col_types,
        pd_timestamp_type="datetime_object",
    )

    test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_datetime"]
    test_str_dates = pd_str_datetime_series_to_list(test_str_dates)

    assert str(df.my_datetime.dtype) == as_datetime_object
    assert isinstance(df.my_datetime[0], datetime.datetime)
    actual_str_dates = df.my_datetime.apply(datetime_object_as_str)
    actual_str_dates = pd_str_datetime_series_to_list(actual_str_dates)
    assert test_str_dates == actual_str_dates

    # pd timestamp
    df = pa_read_csv_to_pandas(
        test_data_path,
        test_col_types=test_col_types,
        pd_timestamp_type="pd_timestamp",
    )

    assert str(df.my_datetime.dtype) == as_pd_timestamp
    actual_str_dates = df.my_datetime.dt.strftime("%Y-%m-%d %H:%M:%S")
    actual_str_dates = pd_str_datetime_series_to_list(actual_str_dates)
    assert test_str_dates == actual_str_dates

    # pd period
    df = pa_read_csv_to_pandas(
        test_data_path,
        test_col_types=test_col_types,
        pd_timestamp_type="pd_period",
    )
    assert str(df.my_datetime.dtype) == as_pd_period
    actual_str_dates = df.my_datetime.apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    actual_str_dates = pd_str_datetime_series_to_list(actual_str_dates)
    assert test_str_dates == actual_str_dates

### ### ### ### ###
### DATES TEST ###
### ### ### ### ###
@pytest.mark.parametrize(
    "in_type,as_datetime_object,as_pd_timestamp,as_pd_period",
    [
        ("date64", "object", "datetime64[ns]", "period[L]"),
        ("date32", "object", "datetime64[ns]", "object")
    ],
)
def test_date(in_type, as_datetime_object, as_pd_timestamp, as_pd_period):

    test_data_path = "tests/data/date_type.csv"
    test_col_types = {"my_date": getattr(pa, in_type)()}

    # datetime_object
    df = pa_read_csv_to_pandas(
        test_data_path,
        test_col_types=test_col_types,
        pd_date_type="datetime_object",
    )

    test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_date"]
    test_str_dates = pd_str_datetime_series_to_list(test_str_dates)

    assert str(df.my_date.dtype) == as_datetime_object
    assert isinstance(df.my_date[0], datetime.date)
    actual_str_dates = df.my_date.apply(datetime_object_as_str)
    actual_str_dates = pd_str_datetime_series_to_list(actual_str_dates)
    assert test_str_dates == actual_str_dates

    # pd timestamp
    df = pa_read_csv_to_pandas(
        test_data_path,
        test_col_types=test_col_types,
        pd_date_type="pd_timestamp",
    )

    assert str(df.my_date.dtype) == as_pd_timestamp
    actual_str_dates = df.my_date.dt.strftime("%Y-%m-%d")
    actual_str_dates = pd_str_datetime_series_to_list(actual_str_dates)
    assert test_str_dates == actual_str_dates

    # pd period
    df = pa_read_csv_to_pandas(
        test_data_path,
        test_col_types=test_col_types,
        pd_date_type="pd_period",
    )
    assert str(df.my_date.dtype) == as_pd_period
    #actual_str_dates = df.my_date.apply(lambda x: x.strftime("%Y-%m-%d"))
    #actual_str_dates = pd_str_datetime_series_to_list(actual_str_dates)
    #assert test_str_dates == actual_str_dates
