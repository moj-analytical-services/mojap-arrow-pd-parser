# mojap-arrow-pd-parser

Using pyArrow/Pandas to read CSV, JSONL and Parquet ensuring that you get the best representation of the column types in the resulting Pandas dataframe. Also ensures data type conformance by maintaining column types when reading and writing data back into Pandas (even when round tripping across multiple data types).

This package also can read in data given a pyArrow schema or [MoJ-Metadata](https://github.com/moj-analytical-services/mojap-metadata) schema again to result in a Pandas dataframe that best represents the provided schema.

Can also be used to write data back to supported formats using Pandas (for CSV/JSONL) and Pyarrow (for Parquet).

## Installation

```
pip install arrow-pd-parser
```

or via GitHub

```
pip install arrow-pd-parser @ git+https://github.com/moj-analytical-services/mojap-arrow-pd-parser
```

## Usage

This package uses `pyArrow` and/or Pandas to parse CSVs, JSONL and Parquet files and convert them to a Pandas Dataframe that are the best representation of those datatypes and ensure conformance between them. Also can write data back into the above formats to still maintain conformance to the provided schema.

```python
from arrow_pd_parser.parse import (
    pa_read_csv_to_pandas,
    pd_read_csv,
)

from arrow_pd_parser.pandas import pd_parser

df1 = pa_read_csv_to_pandas("tests/data/all_types.csv")
df1.dtypes()

# i                     Int64
# my_bool             boolean
# my_nullable_bool    boolean
# my_date              object
# my_datetime          object
# my_int                Int64
# my_string            string

df2 = pd_read_csv("tests/data/all_types.csv")
df2.dtypes() == df1.dtypes()  # True
```

Note that the default behavior of this package is to utilse the new pandas datatypes for Integers, Booleans and Strings that represent Nulls as `pd.NA()`. Dates are returned as nullable objects of `datetime.date()` type and timestamps are `datetime.datetime()`. By default we enforce these types instead of the native pandas timestamp as the indexing for the Pandas timestamp is nanoseconds and can cause dates to be out of bounds. See the [timestamps](#timestamps) section for more details.

If unsure on what parser to use we would suggest:

| Data Type | Parser |
|-----------|--------|
| CSV       | Pandas |
| JSONL     | Pandas |
| Parquet   | Arrow  |


### Advanced Usage

This package will read in tabular data using pyarrow and then convert it to a pandas dataset. The full order of operations are:
- Read in data to an arrow dataset
- Cast the arrow dataset to the specified schema (if provided)
- Convert the arrow dataset to Pandas

#### Reading and Schema Casting (Arrow)

You can split up the previous code example (which used `pa_read_csv_to_pandas`) into two parts to get the exact same result (in case you wanted to do some transformations to the arrow dataset first).

```python
from arrow_pd_parser.parse import pa_read_csv
from arrow_pd_parser.pa_pd import arrow_to_pandas

# Read in the data first then convert it to a pandas dataframe
df_arrow = pa_read_csv("tests/data/all_types.csv")
df = arrow_to_pandas(df_arrow)
```

You can specify the additional parameters used by the pyarrow readers when reading in the data. 

```python
from io import BytesIO
from pyarrow import csv
from arrow_pd_parser.parse import pa_read_csv_to_pandas

csv_data = b"""
a;b
1;"This is text with a 
new line"
2;some more text
"""
test_file = BytesIO(csv_data)
csv_parse_options = csv.ParseOptions(delimiter=";", newlines_in_values=True)
df = pa_read_csv_to_pandas(test_file, parse_options=csv_parse_options)
df.head()
```

You can also provide an arrow schema to try and cast the data.

```python
from io import BytesIO
import pyarrow as pa
from arrow_pd_parser.parse import pa_read_csv_to_pandas


csv_data = b"""
a,b
1,1.24
2,7.81
"""
test_file = BytesIO(csv_data)
from io import BytesIO
import pyarrow as pa
from arrow_pd_parser.parse import pa_read_csv_to_pandas

csv_data = b"""
a,b
1,1.24
2,7.81
"""

# By default the data above would read as
# a:Int64, b:float64 for a Pandas dataframe
# Instead tell pa_pd_parser to treat these with the following schema
schema = pa.schema([("a", pa.string()), ("b", pa.decimal128(3,2))])
test_file = BytesIO(csv_data)
df = pa_read_csv_to_pandas(test_file, schema=schema)
df.types # a: String, b:object (each value is a decimal.Decimal)
```

Note there are currently some issues around pyarrow not being able to cast timestamps to strings (see this see repo's issues for more details). For example:

```python
from io import BytesIO
import pyarrow as pa
from pyarrow import csv
from arrow_pd_parser.parse import pa_read_csv_to_pandas

csv_data = b"""
a,b
1,2020-01-01 00:00:00
2,2021-01-01 23:59:59
"""

# note can also provide partial schema and get package to infer a's type by also setting `expect_full_schema=False`
schema = pa.schema([("b", pa.string())])
test_file = BytesIO(csv_data)

# The following line will raise an ArrowNotImplementedError.
# This is because there is currently no implementation to casting timestamps to str.
df = pa_read_csv_to_pandas(test_file, schema=schema, expect_full_schema=False)

# By default Arrow will read in str representations of timestamps as
# timestamps if they conform to ISO standard format.
# Then you get the error when you try and cast that timestamp to str. To
# get around this you can force pyarrow to read in the data as a string
# when it parses it as a CSV (note that ConvertOptions is not currently
# available for the JSON reader)
co = csv.ConvertOptions(column_types=schema)
df = pa_read_csv_to_pandas(test_file, schema=schema, expect_full_schema=False, convert_options=co)
```

#### Reading and Schema Casting (Pandas)

In the same way you can seperate the reading and casting in the arrow example above you can do the same for the pandas parser.

```python
import pandas as pd
from arrow_pd_parser.parse import (
    pd_read_csv,
    cast_pandas_table_to_schema
)

# Read in the data first then convert it to a pandas dataframe

# pandas parsing/casting only needs type_category except for
# "timestamp" type_categories where both type and type_categories
# are required
meta = {
    "columns": [
        {"name": "my_bool", "type_category": "boolean"},
        {"name": "my_nullable_bool", "type_category": "boolean"},
        {"name": "my_date", "type": "date32", "type_category": "timestamp"},
        {"name": "my_datetime", "type": "timestamp(s)", "type_category": "timestamp"},
        {"name": "my_int", "type_category": "integer"},
        {"name": "my_string", "type_category": "string"},
    ]
}

df_str = pd.read_csv("tests/data/all_types.csv", dtype=str, low_memory=False)  # Best type conversion when reading in types as strings
df_cast = cast_pandas_table_to_schema(df_str, meta)
```

The pandas parser functions that require metadata (like `pd_read_csv` and `cast_pandas_table_to_schema`) takes a `dict` that is compliant with a `Metadata` schema or a `Metadata` object. You can use the metadata object to set type_categories based of column types for metadata that only has the latter:

```python
from io import StringIO
import pandas as pd
from arrow_pd_parser.parse import (
    pd_read_csv,
)

data = """
my_nullable_bool,my_date,my_datetime,my_int
True,True,2013-06-13,2013-06-13 05:11:07,
True,,1995-04-30,1995-04-30 10:23:29,16
False,False,2017-10-15,2017-10-15 20:25:05,0
"""

# Set type categories in metadata object
meta = {
    "columns": [
        {"name": "my_nullable_bool", "type": "bool_"},
        {"name": "my_date", "type": "date32"},
        {"name": "my_datetime", "type": "timestamp(s)"},
        {"name": "my_int", "type": "int64"},
    ]
}
metadata_instance = Metadata.from_dict(meta)
df = pd_read_csv(StringIO(data), metadata_instance)
df.dtypes
# i                     Int64
# my_nullable_bool    boolean
# my_date              object
# my_datetime          object
# my_int                Int64

```


#### Exporting data to CSV/JSON

You can also use the export module of this package to write data back (to CSV and JSON to ensure the same datatype will be read back in). This is useful when having to constantly read/write data between different storage systems and/or pipelines.

```python
from arrow_pd_parser.parse import (
    pa_read_csv_to_pandas,
    pa_read_json_to_pandas,
)
import pyarrow as pa
from io import StringIO, BytesIO
from arrow_pd_parser.export import pd_to_json

s = pa.schema(
    [
        ("i", pa.int8()),
        ("my_bool", pa.bool_()),
        ("my_nullable_bool", pa.bool_()),
        ("my_date", pa.date32()),
        ("my_datetime", pa.timestamp("s")),
        ("my_int", pa.uint8()),
        ("my_string", pa.string()),
    ]
)

# Read in original table
original = pa_read_csv_to_pandas(
        "tests/data/all_types.csv",
        s,
    )

# Write the table back out to a JSONL file
f = StringIO()
pd_to_json(original, f)

# Read it back in and check it matches original
new_f = BytesIO(f.getvalue().encode("utf8"))
new = pa_read_json_to_pandas(new_f, s)

original == new # note that the two False values are where datetime is None in both tables
```

#### Integration with mojap-metadata (Arrow)

The arrow modules can also be used alongside the `mojap-metadata` package which is already installed. In the example below you will need to install the package with the arrow dependencies:

```
pip install mojap-metadata[arrow]
```


```python
from io import BytesIO
from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import (
    ArrowConverter
)
from arrow_pd_parser.parse import pa_read_csv_to_pandas

csv_data = b"""
a,b
1,2020-01-01 00:00:00
2,2021-01-01 23:59:59
"""
test_file = BytesIO(csv_data)

# Define our metadata for the data
md = {
    "name": "test_data",
    "columns": [
        {
            "name": "a",
            "type": "string"
        },
        {
            "name": "b",
            "type": "timestamp(ms)"
        }
    ] 
}
meta = Metadata.from_dict(md)

# Convert our schema to an arrow schema
ac = ArrowConverter()
arrow_schema = ac.generate_from_meta(meta)

# Use the arrow_schema with arrow_pd_parser
df = pa_read_csv_to_pandas(test_file, schema=arrow_schema)
```

## Data Type Conformance

### Timestamps

Pandas timestamps (currently) only support nanosecond resolution which is not ideal for a lot of timestamps as the range can be often too small.

```python
import pandas as pd
pd.Timestamp.min # Timestamp('1677-09-22 00:12:43.145225')

pd.Timestamp.max # Timestamp('2262-04-11 23:47:16.854775807')
```

Whereas Spark 3.0 (for example) allows timestamps from `0001-01-01 00:00:00` to `9999-12-31 23:59:59.999999` ([source](https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html)). By default we do not allow Timestamps for this reason instead we use the python native datetime class as default for our types (wrapped in a Pandas object column type). Users can specify other Pandas date/timestamp using the `pd_timestamp_type` parameter which can either be `object` (default), `pd_timestamp` or `pd_period`. 

When setting `pd_timestamp_type=pd_period` pd_arrow_parser will identify the correct pandas period resolution based on the arrow column type.

```python
from arrow_pd_parser.parse import pa_read_csv_to_pandas
import pyarrow as pa

# The Pandas Period type resolution is determined by the arrow col type's resolution
schema = pa.schema([("my_datetime", pa.timestamp("s"))])
df = pa_read_csv_to_pandas(
    "tests/data/datetime_type.csv",
    pd_timestamp_type="pd_period",
)
df.my_datetime.dtype # "period[S]"

schema = pa.schema([("my_datetime", pa.timestamp("ms"))])
df = pa_read_csv_to_pandas(
    "tests/data/datetime_type.csv",
    pd_timestamp_type="pd_period",
)
df.my_datetime.dtype # "period[L]"


# Using timestamp type
df = pa_read_csv_to_pandas(
    "tests/data/datetime_type.csv",
    pd_timestamp_type="pd_timestamp"
)
df.my_datetime.dtype # dtype('<M8[ns]')
```


