import pyarrow as pa
import pandas as pd


def generate_type_mapper(
    pd_boolean, pd_integer, pd_string, pd_date_type, pd_timestamp_type
):
    tm = {}
    if pd_boolean:
        bool_map = {pa.bool_(): "boolean"}
        tm = {**tm, **bool_map}

    if pd_date_type == "pd_period":
        date_map = {pa.date64: pd.PeriodDtype("ms")}
        tm = {**tm, **date_map}

    if pd_timestamp_type == "pd_period":
        datetime_map = {
            pa.timestamp("s"): pd.PeriodDtype("s"),
            pa.timestamp("ms"): pd.PeriodDtype("ms"),
            pa.timestamp("us"): pd.PeriodDtype("us"),
            pa.timestamp("ns"): pd.PeriodDtype("ns"),
        }
        tm = {**tm, **datetime_map}
    if tm:
        return tm.get
    else:
        return None


def arrow_to_pandas(
    arrow_table,
    pd_boolean=True,
    pd_integer=True,
    pd_string=True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
):
    """Converts arrow table to stricter pandas datatypes based on options.

    Args:
        arrow_table (pa.Table): An arrow table

        new_integer (bool, optional): converts bools to the new pandas BooleanDtype.
        Otherwise will convert to bool (if not nullable) and object of (True, False, None) if nulls exist. Defaults to True.
        
        new_string (bool, optional): [description]. Defaults to True.

        pd_date_type (str, optional): Can be either datetime_object, pd_timestamp or pd_period. Defaults to datetime_object.
        pd_timestamp_type (str, optional): Can be either datetime_object, pd_timestamp or pd_period. Defaults to datetime_object.
    Returns:
        Pandas dataframe with mapped types
    """

    tm = generate_type_mapper(
        pd_boolean, pd_integer, pd_string, pd_date_type, pd_timestamp_type
    )

    timestamp_as_object = pd_timestamp_type == "datetime_object"
    date_as_object = pd_date_type == "datetime_object"

    df = arrow_table.to_pandas(
        types_mapper=tm,
        date_as_object=date_as_object,
        timestamp_as_object=timestamp_as_object,
    )
    return df
