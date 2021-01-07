import datetime
import numpy as np
import pandas as pd

from typing import Union, IO


def pd_to_csv(
    df: pd.DataFrame,
    output_file: Union[IO, str],
    index=False,
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
        if pd.api.types.is_period_dtype(new[col]):
            new[col] = new[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    new.to_csv(output_file, index=index, **kwargs)


def pd_to_json(
    df: pd.DataFrame,
    output_file: Union[IO, str],
    orient="records",
    lines=True,
    indent=4,
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

    for col in new.columns:
        if pd.api.types.is_period_dtype(new[col]):
            new[col] = new[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif any([
            pd.api.types.is_datetime64_any_dtype(new[col]),
            isinstance(
                new[col][new[col].notnull()].iloc[0],
                (datetime.datetime, datetime.date)
            ),
        ]):
            new[col] = new[col].astype(str)
            new[col].replace(["nan", "NaT"], np.nan, regex=False, inplace=True)

    new.to_json(
        output_file, orient=orient, lines=lines, indent=indent, **kwargs
    )
