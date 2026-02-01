# Version History

## 0.3.5.2

* Add application_name

## 0.3.5.1

* Fix chunk_query function

## 0.3.5.0

* Update depends pgcopylib==0.2.3.3
* Update depends pgpack==0.3.2.3
* Update depends sqlparse>=0.5.5
* Change documentation link

## 0.3.4.8

* Add error where table have empty data
* Change server version view
* Remove double quotes from log where table name uses capital letters
* Fix build from source on linux systems

## 0.3.4.7

* Update depends pgcopylib==0.2.3.2
* Update depends pgpack==0.3.2.2
* Fix build from source on unix systems

## 0.3.4.6

* Update depends pgcopylib==0.2.3.1
* Update depends pgpack==0.3.2.1

## 0.3.4.5

* Update depends pgcopylib==0.2.3.0
* Update depends pgpack==0.3.2.0

## 0.3.4.4

* Update depends pgcopylib==0.2.2.8
* Update depends pgpack==0.3.1.7

## 0.3.4.3

* Fix read query with comments in last line
* Update depends pgcopylib==0.2.2.7
* Update depends pgpack==0.3.1.6
* Update depends psycopg_binary>=3.3.2
* Update depends psycopg>=3.3.2
* Back compile depends to cython>=0.29.33
* Make wheels for python 3.10-3.14

## 0.3.4.2

* Update depends pgcopylib==0.2.2.6
* Update depends pgpack==0.3.1.5
* Downgrade compile depends to cython==0.29.33
* Make wheels for python 3.10 and 3.11 only

## 0.3.4.1

* Update depends pgcopylib==0.2.2.5
* Update depends pgpack==0.3.1.4
* Improve invalid byte sequence for encoding "UTF8": 0x00
* Disable Linux Aarch64

## 0.3.4.0

* Update depends pgcopylib==0.2.2.4
* Update depends pgpack==0.3.1.3
* Add auto convert String/FixedString(36) from Clickhouse data to Postgres uuid

## 0.3.3.6

* Update depends pgcopylib==0.2.2.3
* Update depends pgpack==0.3.1.2
* Update depends psycopg>=3.3.0
* Update depends psycopg_binary>=3.3.0
* Update depends sqlparse>=0.5.4
* Fix write_timestamp error Can't subtract offset-naive and offset-aware datetimes

## 0.3.3.5

* Fix diagram destination table
* Refactor PGPackDumper.__write_between()

## 0.3.3.4

* Fix None query error

## 0.3.3.3

* Fix query with limit state metadata read error
* Fix query end with ; metadata read error
* Del chunks after write
* Add gc collect

## 0.3.3.2

* Fix write_between diagram

## 0.3.3.1

* Improve chunk_query function

## 0.3.3.0

* Fix attnum position in metadata (thnx to @Art_Dmitriev)
* Improve Multiquery decorator

## 0.3.2.2

* Update depends pgpack==0.3.1.1
* Update depends psycopg_binary>=3.2.12
* Update depends psycopg==3.2.12

## 0.3.2.1

* Improve get metadata for numeric column without precision (thnx to @Art_Dmitriev)

## 0.3.2.0

* Fix chunk_query for ignoring semicolons inside string literals

## 0.3.1.0

* Update depends pgpack==0.3.1.0

## 0.3.0.0

* Update depends pgpack==0.3.0.9
* Update depends psycopg==3.2.11
* Change PGPackDumper.version to "*version number* gp *version number*" if greenplum detected
* Fix calc read size
* Fix multiquery wrapper
* Add transfer_diagram and DBMetadata to make log diagrams
* Add _dbmeta and _size attributes
* Add log output diagram
* Add auto upload to pip

## 0.2.1.2

* Add dbname.sql & gpversion.sql to queryes directory
* Add PGPackDumper.dbname attribute to detect greenplum or postgres
* Change PGPackDumper.version to "version number|greenplum version number" if greenplum detected

## 0.2.1.1

* Add wheels automake
* Update depends pgpack==0.3.0.8

## 0.2.1.0

* Add *.pyi files for cython module descriptions
* Update MANIFEST.in
* Update depends pgpack==0.3.0.7
* Update depends setuptools==80.9.0

## 0.2.0.7

* Update depends pgpack==0.3.0.6
* Add depends psycopg_binary>=3.2.10
* Add internal methods __read_dump, __write_between and __to_reader to force kwargs creation

## 0.2.0.6

* Add tell() method to CopyReader
* Update requirements.txt depends pgpack==0.3.0.5

## 0.2.0.5

* Delete attribute pos from CopyBuffer
* Add readed and sending size output into log

## 0.2.0.4

* Update requirements.txt depends pgpack==0.3.0.4
* Update requirements.txt depends psycopg==3.2.10
* Fix logger create folder in initialize

## 0.2.0.3

* Change log message
* Improve refresh database after write
* Improve initialization error
* Rename variable result to output

## 0.2.0.2

* Update MANIFEST.in
* Update requirements.txt depends pgpack==0.3.0.3
* Improve pyproject.toml license file approve
* Add CHANGELOG.md to pip package
* Add close files after read/write operations
* Change log messages for read operations

## 0.2.0.1

* Update requirements.txt depends pgpack==0.3.0.2
* Fix multiquery decorator
* Fix pgpack import

## 0.2.0.0

* Redistribute project directories
* Add CopyReader class for read stream
* Add StreamReader class for read same as PGPack stream object
* Add new method to_reader(query, table_name) for get StreamReader
* Add new method from_rows(dtype_data, table_name) for write from python iterable object
* Add new methods from_pandas(data_frame, table_name) & from_polars(data_frame, table_name)
* Add new methods refresh() to refresh session & close() to close PGPackDumper
* Update requirements.txt
* Update README.md
* Change default compressor to ZSTD
* Change CopyBuffer.copy_reader() function
* Delete CopyBuffer read() & tell() functions
* Delete make_buffer_obj method

## 0.1.2.2

* Hotfix root_dir() function

## 0.1.2.1

* Add array nested into metadata
* Add attribute version
* Add more error classes
* Update requirements.txt
* Change initialized message to log
* Change multiquery log

## 0.1.2

* Change metadata structure
* Update requirements.txt

## 0.1.1

* Rename project to pgpack_dumper
* Fix legacy setup.py bdist_wheel mechanism, which will be removed in a future version
* Fix multiquery
* Add CHANGELOG.md

## 0.1.0

* Add CopyBufferObjectError & CopyBufferTableNotDefined
* Add PGObject
* Add logger
* Add sqlparse for cut comments from query
* Add multiquery
* Update requirements.txt

## 0.0.2

* Fix include *.sql
* Fix requirements.txt
* Docs change README.md

## 0.0.1

First version of the library pgcrypt_dumper
