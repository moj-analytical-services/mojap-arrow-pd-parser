import pyarrow as pa
import pandas as pd


def generate_type_mapper(new_integer, new_string):
    tm = {}
    if new_integer:
        int_map = {pa.bool_(): pd.BooleanDtype()}

    if tm:
        return tm.get
    else:
        return None


def arrow_to_pd(arrow_table, new_integer=True, new_string=True, date_type: str, datetime_type: str):
    """Converts arrow table to stricter pandas datatypes based on options.

    Args:
        arrow_table (pa.Table): An arrow table

        new_integer (bool, optional): converts bools to the new pandas BooleanDtype.
        Otherwise will convert to bool (if not nullable) and object of (True, False, None) if nulls exist. Defaults to True.
        
        new_string (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    tm = generate_type_mapper(new_integer, new_string)
    return arrow_table.to_pandas(types_mapper=tm)

