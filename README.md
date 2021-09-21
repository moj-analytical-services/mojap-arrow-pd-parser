# mojap-arrow-pd-parser

Using pyArrow/Pandas to read CSV, JSONL and Parquet ensuring that you get the best representation of the column types in the resulting Pandas dataframe. Also ensures data type conformance by maintaining column types when reading and writing data back into Pandas (even when round tripping across multiple data types).

This package also can read in data given an [MoJ-Metadata](https://github.com/moj-analytical-services/mojap-metadata) schema again to result in a Pandas dataframe that best represents the provided schema.

Can also be used to write data back to supported formats using Pandas (for CSV/JSONL) and PyArrow (for Parquet).

## Installation

```
pip install arrow-pd-parser
```

or via GitHub

```
pip install arrow-pd-parser @ git+https://github.com/moj-analytical-services/mojap-arrow-pd-parser
```

## Basic Usage

This package uses `PyArrow` and/or `Pandas` to parse CSVs, JSONL and Parquet files and convert them to a Pandas Dataframe that are the best representation of those datatypes and ensure conformance between them. Also can write data back into the above formats to still maintain conformance to the provided schema.

```python
from arrow_pd_parser import reader, writer

df1 = reader.read("tests/data/all_types.csv")
df1.dtypes

# i                     Int64
# my_bool             boolean
# my_nullable_bool    boolean
# my_date              object
# my_datetime          object
# my_int                Int64
# my_string            string

df2 = reader.read("tests/data/all_types.jsonl")
df2.dtypes # Note that Pandas struggles with nullable booleans when it reads JSONL (hence it is an Int64)

# Write the dataframe to parquet
# note deafult settings for the parquet writer is to
# compress using snappy. (compression is not inferred from filepath but may do in future releases)
writer.write(df1, "new_output.snappy.parquet")
```

Note that the default behavior of this package is to utilse the new pandas datatypes for Integers, Booleans and Strings that represent Nulls as `pd.NA()`. Dates are returned as nullable objects of `datetime.date()` type and timestamps are `datetime.datetime()`. By default we enforce these types instead of the native pandas timestamp as the indexing for the Pandas timestamp is nanoseconds and can cause dates to be out of bounds. See the [timestamps](#timestamps) section for more details.

The `reader.read()` method will infer what the file format is based on the extension of your filepath failing that it will take the format from your metadata if provided. By default the reader.read() will use the following readers for the prescribed file format.

| Data Type | Reader | Writer |
|-----------|--------|--------|
| CSV       | Pandas | Pandas |
| JSONL     | Pandas | Pandas |
| Parquet   | Arrow  | Arrow  |

You can also specify the file format in the reader and writer function or specify the reader type:

```python
from arrow_pd_parser import reader, writer

# Specifying the reader
# Both reader statements are equivalent and call the same readers
# under the hood
df1 = reader.read("tests/data/all_types.csv", file_format="csv")
df2 = reader.csv.read("tests/data/all_types.csv")

# You can also pass the reader args to the reader as kwargs
df3 = reader.csv.read("tests/data/all_types.csv", nrows = 2)

# The writer API has the same functionality
writer.write(df1, file_format="parquet")
writer.parquet.write(df1)
```

### Use of Metadata

The main usefulness of this repo comes from specifying metadata for the data you want. We are going to focus on CSV and JSONL in this section as Parquet becomes a bit more nuanced as it encodes data types (unlike CSV and JSON that can be more vague).

When reading and writing you can specify metadata (using our [metadata schema definitions](https://github.com/moj-analytical-services/mojap-metadata)) to ensure the data read into pandas conforms.

```python
from arrow_pd_parser import reader

# If no meta is provided we let the reader infer type
df1 = reader.read("tests/data/all_types.csv")
df1.dtypes

# i                     Int64
# my_bool             boolean
# my_nullable_bool    boolean
# my_date              object
# my_datetime          object
# my_int                Int64
# my_string            string

# If metadata is provided we ensure conformance
meta = {
    "columns": [
        {"name": "my_bool", "type": "string"},
        {"name": "my_nullable_bool", "type": "bool"},
        {"name": "my_date", "type": "string"},
        {"name": "my_datetime", "type": "string"},
        {"name": "my_int", "type": "float64"},
        {"name": "my_string", "type": "string"}
    ]
}
df2 = reader.read("tests/data/all_types.csv", metadata=meta)
df2.dtypes

# i                     Int64
# my_bool              string
# my_nullable_bool    boolean
# my_date              string
# my_datetime          string
# my_int              float64
# my_string            string

df3 = reader.read("tests/data/all_types.jsonl", metadata=meta)
df3.dtypes

assert df2.dtypes.to_list() == df3.dtypes.to_list()
```

As of the v1 release we expect the API for reading and writing will remain the same but will still be lacking in how the caster works (what is called under the hood when casting the data to the prescribed metadata). The caster should improve with subsequent releases.

## Advanced Usage

### Parameterising your reader and writer

When using the `reader.read()` or `writer.write()` it pulls the default method with their default settings. However, if you want to customise your reader/writer you can do.


#### Passing arguments to the reader

You can pass arguments to the underlying reader that is used to read data in. In the example below we use the nrows arg in pd.`pd.read_csv` that is used for our underlying reader.

```python
from arrow_pd_parser import reader

# Passing args to the read function.
df = reader.csv.read("tests/data/all_types.jsonl", nrows=1000)
```

#### Reader and Writer settings

The readers and writers have some settings that you can use to configure how it reads/writes data. One of the main settings is how we deal with Pandas types. We default to the new pandas Series types: `StringDtype` for `string`, `BooleanDtype` for `bool` and `Int64Dtype` for `integer`. We also force dates and timestamps to be a series of objects (see the Timestamp section below)[#Reader-Pandas-Timestamps]. To change what pandas types to use you can change the reader settings:

```python
from arrow_pd_parser.reader import csv
from io import StringIO

csv.pd_integer = False
csv.pd_string = False
csv.bool_map = {"Yes": True, "No": False}

data = """
int_col,str_col,bool_col
1,"Hello, mate",Yes
2,Hi,No
"""
meta = {
    "columns": [
        {"name": "int_col", "type": "int64"},
        {"name": "str_col", "type": "string"},
        {"name": "bool_col", "type": "bool"},
    ]
}
f = StringIO(data)
df = csv.read(f, metadata = meta)
df.dtypes
# int_col       int64
# str_col      object
# bool_col    boolean
```

#### Using your own reader

If you wanted to create your own instance of a reader that wasn't the default provided you can.

```python
from arrow_pd_parser.reader import PandasCsvReader, csv


# But you can also create your own and init the settings
specific_csv = PandasCsvReader(
    pd_integer = False,
    pd_string = False,
    bool_map = {"Yes": True, "No": False}
)

# This may come in handy if you want to have two instances of a CSV reader with different settings
df1 = csv.read("tests/data/all_types.csv") # default reader
df2 = specific_csv.read("tests/data/all_types.csv")
```

#### Reading and Writing Parquet

The default parquet reader and writer uses Arrow under the hood. This also means it uses a different method for casting datatypes (it is done in Arrow not with Pandas). Because parquet is stricter most likely if there is a deviation from your metadata it will fail to cast. The when provided metadata reader and writer API forces the data to that metadata, this makes sense for CSV and JSON but may make less sense for parquet. Therefore when using the parquet reader and writer you might want to consider if you want to provide your metadata (this may still be advantagous to check that the parquet matches metadata schema you have for the data).

```python
from arrow_pd_parser import reader, writer

# Note all readers (CSV, JSON and Parquet) support S3 paths in the following way
df = reader.parquet.read("s3://bucket/in.snappy.parquet")
writer.parquet.write("s3://bucket/out.snappy.parquet")

print(writer.parquet.compression, writer.parquet.version)
# The default settings for the Parquet writer are snappy compression and version = '2.0'
# These are passed to the arrow parquet write method but can either be changed by setting the
# properties or by passing them as kwargs (which will super seed the default properties) e.g.
writer.parquet.write("s3://bucket/out.parquet", compression=None, version='1.0')
```

#### Pandas Timestamps

When metadata is provided (or for all Arrow Readers without metadata) we default to dates and datetimes as a series of objects rather than the Pandas timestamps this is because Pandas timestamps (currently) only support nanosecond resolution which is not ideal for a lot of timestamps as the range can be often too small.

```python
import pandas as pd
pd.Timestamp.min # Timestamp('1677-09-22 00:12:43.145225')

pd.Timestamp.max # Timestamp('2262-04-11 23:47:16.854775807')
```

Whereas Spark 3.0 (for example) allows timestamps from `0001-01-01 00:00:00` to `9999-12-31 23:59:59.999999` ([source](https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html)). By default we do not allow Timestamps for this reason instead we use the python native datetime class as default for our types (wrapped in a Pandas object column type). Users can specify other Pandas date/timestamp using the `pd_timestamp_type` parameter which can either be `object` (default), `pd_timestamp` or `pd_period`. 

When setting `pd_timestamp_type=pd_period` pd_arrow_parser will identify the correct pandas period resolution based on the arrow column type.

```python
from arrow_pd_parser.reader import csv

# The Pandas Period
csv.pd_timestamp_type="pd_period"
df = csv.read("tests/data/datetime_type.csv")
df.my_datetime.dtype # "period[S]"

# Use pandas timestamp
csv.pd_timestamp_type="pd_timestamp"
df = csv.read("tests/data/datetime_type.csv")
df.my_datetime.dtype # dtype('<M8[ns]')
```
