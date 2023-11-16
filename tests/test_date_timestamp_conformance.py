import pytest
import datetime
import pandas as pd
import pyarrow as pa
import numpy as np
from arrow_pd_parser._arrow_parsers import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
)


def pd_datetime_series_to_list(s, series_type, date=False):
    fmt = "%Y-%m-%d" if date else "%Y-%m-%d %H:%M:%S"
    if series_type == "object":
        s_ = s.apply(datetime_object_as_str).to_list()
    elif series_type == "datetime64":
        s_ = s.dt.strftime(fmt).to_list()
    elif series_type == "period":
        s_ = s.apply(lambda x: None if pd.isna(x) else x.strftime(fmt))
        s_ = s_.to_list()
    else:
        raise ValueError(f"series_type input {series_type} not expected.")
    str_dates = [None if pd.isna(x) else x for x in s_]
    return str_dates


def datetime_object_as_str(x):
    if pd.isna(x):
        return np.nan
    else:
        return str(x)


@pytest.mark.parametrize(
    "in_type,pd_timestamp_type,out_type",
    [
        ("timestamp[s]", "datetime_object", "object"),
        ("timestamp[s]", "pd_timestamp", "datetime64[s]"),
        ("timestamp[s]", "pd_period", "period[S]"),
        ("timestamp[ms]", "datetime_object", "object"),
        ("timestamp[ms]", "pd_timestamp", "datetime64[ms]"),
        ("timestamp[ms]", "pd_period", "period[L]"),
        ("timestamp[us]", "datetime_object", "object"),
        ("timestamp[us]", "pd_timestamp", "datetime64[us]"),
        ("timestamp[us]", "pd_period", "period[U]"),
        ("timestamp[ns]", "datetime_object", "datetime64[ns]"),
        ("timestamp[ns]", "pd_timestamp", "datetime64[ns]"),
        ("timestamp[ns]", "pd_period", "period[N]"),
    ],
)
def test_datetime(in_type, pd_timestamp_type, out_type):
    test_data_path = "tests/data/datetime_type.csv"
    test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_datetime"]
    test_str_dates = [None if pd.isna(s) else s for s in test_str_dates]

    type_dict = {
        "timestamp[s]": pa.timestamp("s"),
        "timestamp[ms]": pa.timestamp("ms"),
        "timestamp[us]": pa.timestamp("us"),
        "timestamp[ns]": pa.timestamp("ns"),
    }

    schema = pa.schema([("my_datetime", type_dict[in_type])])

    # datetime_object
    df = pa_read_csv_to_pandas(
        test_data_path,
        schema=schema,
        expect_full_schema=False,
        pd_timestamp_type=pd_timestamp_type,
    )

    test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_datetime"]
    test_str_dates = [None if pd.isna(s) else s for s in test_str_dates]

    assert str(df.my_datetime.dtype) == out_type
    if out_type == "object":
        assert isinstance(df.my_datetime[0], datetime.datetime)

    actual_str_dates = pd_datetime_series_to_list(
        df.my_datetime, out_type.split("[")[0], date=False
    )
    assert test_str_dates == actual_str_dates


@pytest.mark.parametrize(
    "pd_timestamp_type,expect_error",
    [("datetime_object", False), ("pd_timestamp", True), ("pd_period", False)],
)
def test_out_of_bounds_datetime(pd_timestamp_type, expect_error):
    test_data_path = "tests/data/datetime_type_oob.csv"
    test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_datetime"]
    test_str_dates = [None if pd.isna(s) else s for s in test_str_dates]

    schema = pa.schema([("my_datetime", pa.timestamp("s"))])

    out_type_lu = {
        "datetime_object": "object",
        "pd_timestamp": "datetime64",
        "pd_period": "period",
    }
    out_type = out_type_lu[pd_timestamp_type]

    # datetime_object
    try:
        df = pa_read_csv_to_pandas(
            test_data_path,
            schema=schema,
            expect_full_schema=False,
            pd_timestamp_type=pd_timestamp_type,
        )
    except pa.lib.ArrowInvalid:
        assert expect_error is True
    else:
        df = pa_read_csv_to_pandas(
            test_data_path,
            schema=schema,
            expect_full_schema=False,
            pd_timestamp_type=pd_timestamp_type,
        )
        test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_datetime"]
        test_str_dates = [None if pd.isna(s) else s for s in test_str_dates]

        if out_type == "object":
            assert isinstance(df.my_datetime[0], datetime.datetime)

        actual_str_dates = pd_datetime_series_to_list(
            df.my_datetime, out_type.split("[")[0], date=False
        )

        assert test_str_dates == actual_str_dates


@pytest.mark.parametrize(
    "in_type,pd_date_type,out_type",
    [
        ("date32", "datetime_object", "object"),
        ("date32", "pd_timestamp", "datetime64[ms]"),
        ("date32", "pd_period", "object"),
        ("date64", "datetime_object", "object"),
        ("date64", "pd_timestamp", "datetime64[ms]"),
        ("date64", "pd_period", "period[L]"),
    ],
)
def test_date(in_type, pd_date_type, out_type):
    test_data_path = "tests/data/date_type.csv"
    test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_date"]
    test_str_dates = [None if pd.isna(s) else s for s in test_str_dates]

    schema = pa.schema([("my_date", getattr(pa, in_type)())])

    # datetime_object
    if in_type == "date32" and pd_date_type == "pd_period":
        with pytest.warns(UserWarning):
            df = pa_read_csv_to_pandas(
                test_data_path,
                schema,
                expect_full_schema=False,
                pd_date_type=pd_date_type,
            )
    else:
        df = pa_read_csv_to_pandas(
            test_data_path, schema, expect_full_schema=False, pd_date_type=pd_date_type
        )

    test_str_dates = pd.read_csv(test_data_path, dtype=str)["my_date"]
    test_str_dates = [None if pd.isna(s) else s for s in test_str_dates]

    assert str(df.my_date.dtype) == out_type
    if out_type == "object":
        assert isinstance(df.my_date[0], datetime.date)

    actual_str_dates = pd_datetime_series_to_list(
        df.my_date, out_type.split("[")[0], date=True
    )
    assert test_str_dates == actual_str_dates


@pytest.mark.skip(
    reason=(
        "This currently fails (see issue #43), but adding in "
        "test boilerplate for a future fix."
    )
)
def test_timestamps_as_strs():
    test_data_path = "tests/data/datetime_type.csv"
    test_str_dates = pd.read_csv(test_data_path, dtype="string")["my_datetime"]

    schema = pa.schema([("my_datetime", pa.string())])
    df = pa_read_csv_to_pandas(test_data_path, schema, expect_full_schema=False)
    assert df["my_datetime"].to_list() == test_str_dates.to_list()

    df = pa_read_json_to_pandas(
        test_data_path.replace(".csv", ".jsonl"), schema, expect_full_schema=False
    )
    assert df["my_datetime"].to_list() == test_str_dates.to_list()
