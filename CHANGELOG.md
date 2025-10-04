# Version History

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
