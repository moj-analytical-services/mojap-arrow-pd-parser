import datetime
import numpy as np
import pandas as pd
import pyarrow as pa

from typing import Union, IO


def pd_to_csv(
    df: pd.DataFrame, output_file: Union[IO, str], index: bool = False, **kwargs,
):
    """Export a dataframe to a csv this package can open identically.

    Converts period data types to datetime strings with precision to the second.

    Args:
        df (pd.DataFrame): a pandas dataframe
        output_file (IO or str): the path you want to export to
        index (bool): standard pandas .to_csv index argument, but defaulting to False
        **kwargs: any other keyword arguments to pass to pandas .to_csv
    """
    new = df.copy()

    for col in new.columns:
        # Convert period columns to strings so they're exported in a way Arrow can read
        if pd.api.types.is_period_dtype(new[col]):
            new[col] = new[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    new.to_csv(output_file, index=index, **kwargs)


def pd_to_json(
    df: pd.DataFrame,
    output_file: Union[IO, str],
    orient: str = "records",
    lines: bool = True,
    **kwargs,
):
    """Export a dataframe to a json newlines file this package can open identically.

    Converts period data types to datetime strings with precision to the second.

    Args:
        df (pd.DataFrame): a pandas dataframe
        output_file (IO or str): the path you want to export to
        index (bool): standard pandas .to_json index argument, but defaulting to False
        orient (str): standard pandas .to_json orient argument, defaulting to 'records'
        lines (bool): standard pandas .to_json lines argument, defaulting to True
        **kwargs: any other keyword arguments to pass to pandas .to_json
    """
    new = df.copy()

    # Convert date-related columns to strings Arrow can read consistently
    for col in new.columns:
        if pd.api.types.is_period_dtype(new[col]):
            new[col] = new[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif any(
            [
                pd.api.types.is_datetime64_any_dtype(new[col]),
                isinstance(
                    new[col][new[col].notnull()].iloc[0],
                    (datetime.datetime, datetime.date),
                ),
            ]
        ):
            new[col] = new[col].astype(pd.StringDtype())
            # Convert pd_timestamp string 'NaT' to NaN so PyArrow can read them
            new[col].replace("NaT", np.nan, regex=False, inplace=True)

    new.to_json(output_file, orient=orient, lines=lines, **kwargs)


def pd_to_parquet(
    df: pd.DataFrame,
    output_file: Union[IO, str],
    arrow_schema: pa.lib.Schema,
):
    """
    Export a data frame as parquet
    Does no conversion, and adheres to the provided schema

    Args:
        df (pd.DataFrame): a pandas dataframe
        output_file (IO or str): the path you want to export to
    """
    if not isinstance(arrow_schema, pa.lib.schema):
        raise TypeError(
            f"arrow schema must be pyarrow schema, found {type(arrow_schema)}"
        )
    table = pa.Table.from_pandas(df)
    table = table.cast(arrow_schema)

    if isinstance(output_file, str):
        s3 = pa.fs.S3FileSystem(region='eu-west-1')
        with s3.open_output_stream(output_file.replace("s3://","")) as f:
            pa.parquet.write_table(table, f)
    elif isinstance(output_file, IO):
        pa.parquet.write_table(table, output_file)
    else:
        raise TypeError(f"output file must be IO or str, found {type(output_file)}")
