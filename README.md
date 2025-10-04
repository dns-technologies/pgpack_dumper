# PGPackDumper

Library for read and write PGPack format between PostgreSQL and file

## Examples

### Initialization

```python
from pgpack_dumper import (
    CompressionMethod,
    PGConnector,
    PGPackDumper,
)

connector = PGConnector(
    host = <your host>,
    dbname = <your database>,
    user = <your username>,
    password = <your password>,
    port = <your port>,
)

dumper = PGPackDumper(
    connector=connector,
    compression_method=CompressionMethod.ZSTD,  # or CompressionMethod.LZ4 or CompressionMethod.NONE
)
```

### Read dump from PostgreSQL into file

```python
file_name = "test_table.pgpack"
# you need define one of parameter query or table_name
query = "select ..."  # some sql query
table_name = "public.test_table"  # or some table

with open(file_name, "wb") as fileobj:
    dumper.read_dump(
        fileobj,
        query,
        table_name,
    )
```

### Write dump from file into PostgreSQL

```python
file_name = "test_table.pgpack"
# you need define one of parameter table_name
table_name = "public.test_table"  # some table

with open(file_name, "rb") as fileobj:
    dumper.write_dump(
        fileobj,
        table_name,
    )
```

### Write from PostgreSQL into PostgreSQL

Same server

```python

table_dest = "public.test_table_write"  # some table for write
table_src = "public.test_table_read"  # some table for read
query_src = "select ..."  # or some sql query for read

dumper.write_between(
    table_dest,
    table_src,
    query_src,
)
```

Different servers

```python

connector_src = PGConnector(
    host = <host src>,
    dbname = <database src>,
    user = <username src>,
    password = <password src>,
    port = <port src>,
)

dumper_src = PGPackDumper(connector=connector_src)

table_dest = "public.test_table_write"  # some table for write
table_src = "public.test_table_read"  # some table for read
query_src = "select ..."  # or some sql query for read

dumper.write_between(
    table_dest,
    table_src,
    query_src,
    dumper_src,
)
```

### Get stream object

```python
# you need define one of parameter query or table_name
query = "select ..."  # some sql query
table_name = "public.test_table"  # or some table
reader = dumper.to_reader(query=query, table_name=table_name)
print(reader)
# <PostgreSQL/GreenPlum stream reader>
# ┌─────────────────┬─────────────────┐
# │ Column Name     │ PostgreSQL Type │
# ╞═════════════════╪═════════════════╡
# │ column1         │ date            │
# │-----------------+-----------------│
# │ column2         │ bpchar          │
# │-----------------+-----------------│
# │ column3         │ bpchar          │
# └─────────────────┴─────────────────┘
# Total columns: 3
# Readed rows: 0
```

StreamReader has three methods available,
but only one of the methods is available at a time within a single session.

```python
# read as python generator object
reader.to_rows()
# or read as pandas.DataFrame
reader.to_pandas()
# or read as polars.DataFrame
reader.to_polars()
```

### Write from python objects into target table

```python
# some table for write data
table_name = "public.test_table"
dtype_data: Itarable[Any]
pandas_frame: pandas.DataFrame
polars_frame: polars.DataFrame

# write from python object
dumper.from_rows(dtype_data, table_name)
# write from pandas.DataFrame
dumper.from_pandas(pandas_frame, table_name)
# write from polars.DataFrame
dumper.from_polars(polars_frame, table_name)
```

### Open PGPack file format

Get info from my another repository https://github.com/0xMihalich/pgpack

## Installation

### From pip

```bash
pip install pgpack-dumper
```

### From local directory

```bash
pip install .
```

### From git

```bash
pip install git+https://github.com/0xMihalich/pgpack_dumper
```
