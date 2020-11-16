import pyarrow as pa
import pandas as pd


def generate_type_mapper(pd_boolean, pd_integer, pd_string):
    tm = {}
    if pd_boolean:
        bool_map = {pa.bool_(): pd.BooleanDtype()}
        tm = {**tm, **bool_map}
    if pd_string:
        string_map = {pa.string(): pd.StringDtype()}
        tm = {**tm, **string_map}

    if tm:
        return tm.get
    else:
        return None


def arrow_to_pandas(
    arrow_table,
    pd_boolean=True,
    pd_integer=True,
    pd_string=True,
    date_type: str = None,
    datetime_type: str = None,
):
    """Converts arrow table to stricter pandas datatypes based on options.

    Args:
        arrow_table (pa.Table): An arrow table

        pd_boolean (bool, optional): converts bools to the new pandas BooleanDtype.
        Otherwise will convert to bool (if not nullable) and object of (True, False, None) if nulls exist. Defaults to True.

        pd_string (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    tm = generate_type_mapper(pd_boolean, pd_integer, pd_string)
    return arrow_table.to_pandas(types_mapper=tm, date_as_object=True)
