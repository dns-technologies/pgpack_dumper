# PGPackDumper

Library for read and write PGPack format between PostgreSQL/Greenplum and file

## Features

- Read/write data between PostgreSQL/Greenplum and PGPack files
- Transfer data directly between different database servers
- Stream processing with minimal memory footprint
- Support for BINARY and CSV formats
- Multiple compression methods (ZSTD, GZIP, LZ4, SNAPPY)
- PostgreSQL COPY binary format support via ```pgpack```
- Pandas and Polars integration
- Debug mode with query execution plans
- Read-only mode detection

## Installation

### From pip

```bash
pip install pgpack-dumper -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From local directory

```bash
pip install . --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From git

```bash
pip install git+https://github.com/dns-technologies/pgpack_dumper --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

## Quick Start

### Initialization

```python
from pgpack_dumper import (
    CompressionMethod,
    DumpFormat,
    DumperMode,
    PGConnector,
    PGPackDumper,
)

connector = PGConnector(
    host="localhost",
    dbname="testdb",
    user="postgres",
    password="password",
    port=5432,
)

dumper = PGPackDumper(
    connector=connector,
    compression_method=CompressionMethod.ZSTD,
    compression_level=3,
    dump_format=DumpFormat.BINARY,
    mode=DumperMode.PROD,
)
```

### Read dump from PostgreSQL into file

```python
file_name = "test_table.pgpack"
# use either query or table_name
query = "SELECT * FROM users WHERE age > 21"
table_name = "public.users"

with open(file_name, "wb") as fileobj:
    dumper.read_dump(fileobj, query=query, table_name=table_name)
```

### Write dump from file into PostgreSQL

```python
file_name = "test_table.pgpack"
table_name = "public.users_copy"

with open(file_name, "rb") as fileobj:
    dumper.write_dump(fileobj, table_name)
```

### Transfer data between PostgreSQL databases

**Same server:**

```python
dumper.write_between(
    table_dest="public.users_backup",
    table_src="public.users",
)
```

**Different servers:**

```python
connector_src = PGConnector(
    host="source-host",
    dbname="sourcedb",
    user="source_user",
    password="source_pass",
    port=5432,
)

dumper_src = PGPackDumper(connector=connector_src)

dumper.write_between(
    table_dest="public.users_copy",
    table_src="public.users",
    dumper_src=dumper_src,
)
```

### Stream reader

```python
reader = dumper.to_reader(table_name="public.users")

# Get as Python rows generator
for row in reader.to_rows():
    print(row)

# Get as pandas DataFrame
df = reader.to_pandas()

# Get as polars DataFrame
pl_df = reader.to_polars()
```

### Write from Python objects

```python
# From rows (tuples or lists)
rows = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
dumper.from_rows(rows, "public.users")

# From pandas DataFrame
import pandas as pd
df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
dumper.from_pandas(df, "public.users")

# From polars DataFrame
import polars as pl
pl_df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
dumper.from_polars(pl_df, "public.users")
```

### Get table metadata

```python
# Get metadata as DBMetadata object
metadata = dumper.metadata(table_name="public.users")
print(metadata.columns)  # OrderedDict with column names and types
print(metadata.version)  # PostgreSQL/Greenplum version

# Get raw metadata bytes (for internal use)
raw_metadata = dumper.metadata(table_name="public.users", reader_meta=True)
```

## Configuration

### Compression Methods

| Method | Description |
|--------|-------------|
| ```CompressionMethod.NONE``` | No compression |
| ```CompressionMethod.GZIP``` | GZIP compression |
| ```CompressionMethod.LZ4``` | LZ4 compression (fast) |
| ```CompressionMethod.SNAPPY``` | Snappy compression |
| ```CompressionMethod.ZSTD``` | Zstandard compression (default, best ratio) |

### Dump Formats

| Format | Description |
|--------|-------------|
| ```DumpFormat.BINARY``` | PostgreSQL COPY binary format (default) |
| ```DumpFormat.CSV``` | CSV format with type preservation |

### Dumper Modes

| Mode | Description |
|------|-------------|
| ```DumperMode.PROD``` | Production mode - normal operation |
| ```DumperMode.DEBUG``` | Debug mode - shows query execution plans and diagrams |
| ```DumperMode.TEST``` | Test mode - validates without writing data |

## Class Reference

### PGPackDumper

Main class for database operations.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| ```connector``` | ```PGConnector``` | required | Database connection parameters |
| ```compression_method``` | ```CompressionMethod``` | ```ZSTD``` | Compression algorithm |
| ```compression_level``` | ```int``` | ```3``` | Compression level (1-22 for ZSTD) |
| ```logger``` | ```Logger``` | ```None``` | Custom logger instance |
| ```timeout``` | ```int``` | ```None``` | Statement timeout in seconds |
| ```isolation``` | ```IsolationLevel``` | ```committed``` | Transaction isolation level |
| ```mode``` | ```DumperMode``` | ```PROD``` | Dumper operation mode |
| ```dump_format``` | ```DumpFormat``` | ```BINARY``` | Output format |
| ```s3_file``` | ```bool``` | ```False``` | S3 streaming mode |

**Properties:**

| Property | Description |
|----------|-------------|
| ```dump_format``` | Output format |
| ```timeout``` | Get/set statement timeout |
| ```isolation``` | Get/set transaction isolation level |

**Parameters:**

| Name | Description |
|----------|-------------|
| ```is_readonly``` | Check if connected to read-only server |

**Methods:**

| Method | Description |
|--------|-------------|
| ```read_dump(fileobj, query, table_name)``` | Read data to file |
| ```write_dump(fileobj, table_name)``` | Write data from file |
| ```write_between(table_dest, table_src, query_src, dumper_src)``` | Transfer between databases |
| ```to_reader(query, table_name)``` | Get stream reader |
| ```from_rows(rows, table_name)``` | Write from Python rows |
| ```from_pandas(df, table_name)``` | Write from pandas DataFrame |
| ```from_polars(df, table_name)``` | Write from polars DataFrame |
| ```from_bytes(bytes_data, table_name)``` | Write from bytes chunks |
| ```metadata(query, table_name, reader_meta)``` | Get table metadata |
| ```refresh()``` | Refresh database connection |
| ```close()``` | Close connection |

### PGConnector

Connection parameters container.

```python
PGConnector(
    host="localhost",
    port=5432,
    user="postgres",
    password="password",
    dbname="testdb",
)
```

## Debug Mode Features

When ```mode=DumperMode.DEBUG```, the dumper provides:

- **Query execution plans** - EXPLAIN ANALYZE output for SELECT/INSERT/UPDATE/DELETE
- **Data transfer diagrams** - Visual representation of source and destination schemas
- **Type mismatch detection** - Shows column type differences between source and target

Example debug output:

```text
Transfer data diagram:
┌───────────────────────────┐        ┌────────────────────────────────┐
│ Source [postgres 15.15]   │        │ Destination [postgres 15.15]   │
╞═══════════════╤═══════════╡  │╲    ╞═════════════════╤══════════════╡
│ Column Name   │ Data Type │  │ ╲   │ Column Name     │ Data Type    │
╞═══════════════╪═══════════╡ ┌┘  ╲  ╞═════════════════╪══════════════╡
│ name          │ text      │ │    ╲ │ name            │ text         │
├───────────────┼───────────┤ │    ╱ ├─────────────────┼──────────────┤
│ age           │ int4      │ └┐  ╱  │ age             │ int4         │
└───────────────┴───────────┘  │ ╱   └─────────────────┴──────────────┘
                               │╱
```

## Dependencies

- Python >= 3.10
- ```base_dumper``` >= 0.2.0.dev4
- ```csvpack``` >= 0.1.0.dev4
- ```pgpack``` >= 0.3.3.dev5
- ```psycopg``` >= 3.3.3
- ```light-compressor``` >= 0.1.1.dev1

## License

MIT
