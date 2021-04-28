import datetime
import numpy as np
import pandas as pd
import pyarrow as pa
import warnings

from typing import Union, IO


def pd_to_csv(
    df: pd.DataFrame,
    output_file: Union[IO, str],
    index: bool = False,
    **kwargs,
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
    output_file: Union[str, pa.lib.NativeFile],
    from_pandas_kwargs={},
    write_table_kwargs={},
    arrow_schema: pa.lib.Schema = None,
):
    """
    Export a data frame as parquet
    Does no conversion, and adheres to the parquet schema

    Args:
        df (pd.DataFrame): a pandas dataframe
        output_file (str): the path you want to export to (s3)
        from_pandas_kwargs (optional, dict): kwargs to pass to pyarrow.Table.from_pandas
        write_table_kwargs (optional, dicr):
            kwargs to pass to pyarrow.parquet.write_table
        arrow_schema (optional, pyarrow.lib.schema): schema to cast the dataframe to
            during conversion
    """

    if from_pandas_kwargs.get("schema") and arrow_schema is not None:
        warnings.warn(
            "schema specified twice, dropping schema specified in from_pandas_kwargs"
        )
        from_pandas_kwargs.pop("schema")

    if type(output_file) != str and type(output_file) != pa.lib.NativeFile:
        raise TypeError(f"unsupported output type: {type(output_file)}")

    table = pa.Table.from_pandas(df, **from_pandas_kwargs, schema=arrow_schema)

    # there was some date converting here, but I think parquet is ok as is
    # ... I think ...

    pa.parquet.write_table(table, output_file, **write_table_kwargs)
