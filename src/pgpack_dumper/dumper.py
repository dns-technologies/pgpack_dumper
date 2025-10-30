from collections import OrderedDict
from collections.abc import Generator
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from types import MethodType
from typing import (
    Any,
    Iterable,
    Iterator,
    Union,
)

from pgcopylib import PGCopyWriter
from pgpack import (
    CompressionMethod,
    PGPackReader,
    PGPackWriter,
    metadata_reader,
)
from psycopg import (
    Connection,
    Copy,
    Cursor,
)
from pandas import DataFrame as PdFrame
from polars import DataFrame as PlFrame
from sqlparse import format as sql_format

from .common import (
    CopyBuffer,
    DBMetadata,
    DumperLogger,
    PGConnector,
    PGPackDumperError,
    PGPackDumperReadError,
    PGPackDumperWriteError,
    PGPackDumperWriteBetweenError,
    StreamReader,
    chunk_query,
    make_columns,
    query_template,
    transfer_diagram,
)


class PGPackDumper:
    """Class for read and write PGPack format."""

    def __init__(
        self,
        connector: PGConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        logger: Logger | None = None,
    ) -> None:
        """Class initialization."""

        if not logger:
            logger = DumperLogger()

        try:
            self.connector: PGConnector = connector
            self.compression_method: CompressionMethod = compression_method
            self.logger = logger
            self.connect: Connection = Connection.connect(
                **self.connector._asdict()
            )
            self.cursor: Cursor = self.connect.cursor()
            self.copy_buffer: CopyBuffer = CopyBuffer(self.cursor, self.logger)
            self._dbmeta: DBMetadata | None = None
            self._size = 0
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperError(error)

        self.cursor.execute(query_template("dbname"))
        self.dbname = self.cursor.fetchone()[0]
        self.version = (
            f"{self.connect.info.server_version // 10000}."
            f"{self.connect.info.server_version % 1000}"
        )

        if self.dbname == "greenplum":
            self.cursor.execute(query_template("gpversion"))
            gpversion = self.cursor.fetchone()[0]
            self.version = f"{self.version} gp {gpversion}"

        self.logger.info(
            f"PGPackDumper initialized for host {self.connector.host}"
            f"[version {self.version}]"
        )

    @staticmethod
    def multiquery(dump_method: MethodType):
        """Multiquery decorator."""

        def wrapper(*args, **kwargs):

            first_part: list[str]
            second_part: list[str]

            self: PGPackDumper = args[0]
            cursor: Cursor = (kwargs.get("dumper_src") or self).cursor
            query: str = kwargs.get("query_src") or kwargs.get("query")
            part: int = 1
            first_part, second_part = chunk_query(self.query_formatter(query))
            total_prts = len(sum((first_part, second_part), [])) + int(
                bool(kwargs.get("table_name") or kwargs.get("table_src"))
            )

            if len(first_part) > 1:
                for query in first_part:
                    self.logger.info(f"Execute query {part}/{total_prts}")
                    cursor.execute(query)
                    part += 1

            if second_part:
                for key in ("query", "query_src"):
                    if key in kwargs:
                        kwargs[key] = second_part.pop(0)
                        break

            self.logger.info(
                f"Execute stream {part}/{total_prts} [pgcopy mode]"
            )
            output = dump_method(*args, **kwargs)

            if second_part:
                for query in second_part:
                    part += 1
                    self.logger.info(f"Execute query {part}/{total_prts}")
                    cursor.execute(query)

            if output:
                self.refresh()

            return output

        return wrapper

    def query_formatter(self, query: str) -> str | None:
        """Reformat query."""

        if not query:
            return
        return sql_format(sql=query, strip_comments=True).strip().strip(";")

    @multiquery
    def __read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None,
        table_name: str | None,
    ) -> bool:
        """Internal method read_dump for generate kwargs to decorator."""

        def __read_data(
            copy_to: Iterator[Copy],
        ) -> Generator[bytes, None, None]:
            """Generate bytes from copy object with calc size."""

            self._size = 0

            for data in copy_to:
                chunk = bytes(data)
                self._size += len(chunk)
                yield chunk

        try:
            self.copy_buffer.query = query
            self.copy_buffer.table_name = table_name
            metadata = self.copy_buffer.metadata
            pgpack = PGPackWriter(
                fileobj,
                metadata,
                self.compression_method,
            )
            columns = make_columns(*metadata_reader(metadata))
            source = DBMetadata(
                name=self.dbname,
                version=self.version,
                columns=columns,
            )
            destination = DBMetadata(
                name="file",
                version=fileobj.name,
                columns=columns,
            )
            self.logger.info(transfer_diagram(source, destination))

            with self.copy_buffer.copy_to() as copy_to:
                pgpack.from_bytes(__read_data(copy_to))

            pgpack.close()
            self.logger.info(f"Successfully read {self._size} bytes.")
            self.logger.info(
                f"Read pgpack dump from {self.connector.host} done."
            )
            return True
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperReadError(error)

    @multiquery
    def __write_between(
        self,
        table_dest: str,
        table_src: str | None,
        query_src: str | None,
        dumper_src: Union["PGPackDumper", object],
    ) -> bool:
        """Internal method write_between for generate kwargs to decorator."""

        try:
            if not dumper_src:
                connect = Connection.connect(**self.connector._asdict())
                self.logger.info(
                    f"Set new connection for host {self.connector.host}."
                )
                source_copy_buffer = CopyBuffer(
                    connect.cursor(),
                    self.logger,
                    query_src,
                    table_src,
                )
                src_dbname = self.dbname
                src_version = self.version
            elif dumper_src.__class__ is PGPackDumper:
                source_copy_buffer = dumper_src.copy_buffer
                source_copy_buffer.table_name = table_src
                source_copy_buffer.query = query_src
                src_dbname = dumper_src.dbname
                src_version = dumper_src.version
            else:
                reader = dumper_src.to_reader(
                    query=query_src,
                    table_name=table_src,
                )
                dtype_data = reader.to_rows()
                self.from_rows(
                    dtype_data=dtype_data,
                    table_name=table_dest,
                    source=dumper_src._dbmeta,
                )
                size = reader.tell()
                self.logger.info(f"Successfully sending {size} bytes.")
                return reader.close()

            self.copy_buffer.table_name = table_dest
            source = DBMetadata(
                name=src_dbname,
                version=src_version,
                columns=make_columns(
                    *metadata_reader(source_copy_buffer.metadata),
                ),
            )
            destination = DBMetadata(
                name=self.dbname,
                version=self.version,
                columns=make_columns(
                    *metadata_reader(self.copy_buffer.metadata),
                ),
            )
            self.logger.info(transfer_diagram(source, destination))
            self.copy_buffer.copy_between(source_copy_buffer)
            self.connect.commit()
            return True
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperWriteBetweenError(error)

    @multiquery
    def __to_reader(
        self,
        query: str | None,
        table_name: str | None,
    ) -> StreamReader:
        """Internal method to_reader for generate kwargs to decorator."""

        self.copy_buffer.query = query
        self.copy_buffer.table_name = table_name
        metadata = self.copy_buffer.metadata
        self._dbmeta = DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=make_columns(
                *metadata_reader(metadata),
            ),
        )
        return StreamReader(
            metadata,
            self.copy_buffer.copy_to(),
        )

    def read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None = None,
        table_name: str | None = None,
    ) -> bool:
        """Read PGPack dump from PostgreSQL/GreenPlum."""

        return self.__read_dump(
            fileobj=fileobj,
            query=query,
            table_name=table_name,
        )

    def write_dump(
        self,
        fileobj: BufferedReader,
        table_name: str,
    ) -> None:
        """Write PGPack dump into PostgreSQL/GreenPlum."""

        try:
            self.copy_buffer.table_name = table_name
            pgpack = PGPackReader(fileobj)
            source = DBMetadata(
                name="file",
                version=fileobj.name,
                columns=make_columns(
                    pgpack.columns,
                    pgpack.pgtypes,
                    pgpack.pgparam,
                ),
            )
            destination = DBMetadata(
                name=self.dbname,
                version=self.version,
                columns=make_columns(
                    *metadata_reader(self.copy_buffer.metadata),
                ),
            )
            self.logger.info(transfer_diagram(source, destination))
            self.copy_buffer.copy_from(pgpack.to_bytes())
            self.connect.commit()
            pgpack.close()
            self.refresh()
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperWriteError(error)

    def write_between(
        self,
        table_dest: str,
        table_src: str | None = None,
        query_src: str | None = None,
        dumper_src: Union["PGPackDumper", object] = None,
    ) -> None:
        """Write from PostgreSQL/GreenPlum into PostgreSQL/GreenPlum."""

        return self.__write_between(
            table_dest=table_dest,
            table_src=table_src,
            query_src=query_src,
            dumper_src=dumper_src,
        )

    def to_reader(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> StreamReader:
        """Get stream from PostgreSQL/GreenPlum as StreamReader object."""

        return self.__to_reader(
            query=query,
            table_name=table_name,
        )

    def from_rows(
        self,
        dtype_data: Iterable[Any],
        table_name: str,
        source: DBMetadata | None = None,
    ) -> None:
        """Write from python iterable object
        into PostgreSQL/GreenPlum table."""

        if not source:
            source = DBMetadata(
                name="python",
                version="iterable object",
                columns={"Unknown": "Unknown"},
            )

        self.copy_buffer.table_name = table_name
        columns, pgtypes, pgparam = metadata_reader(self.copy_buffer.metadata)
        writer = PGCopyWriter(None, pgtypes)
        destination = DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=make_columns(
                list_columns=columns,
                pgtypes=pgtypes,
                pgparam=pgparam,
            ),
        )
        self.logger.info(transfer_diagram(source, destination))
        self.copy_buffer.copy_from(writer.from_rows(dtype_data))
        self.connect.commit()
        self.refresh()

    def from_pandas(
        self,
        data_frame: PdFrame,
        table_name: str,
    ) -> None:
        """Write from pandas.DataFrame into PostgreSQL/GreenPlum table."""

        self.from_rows(
            dtype_data=iter(data_frame.values),
            table_name=table_name,
            source=DBMetadata(
                name="pandas",
                version="DataFrame",
                columns=OrderedDict(zip(
                    data_frame.columns,
                    [str(dtype) for dtype in data_frame.dtypes],
                )),
            )
        )

    def from_polars(
        self,
        data_frame: PlFrame,
        table_name: str,
    ) -> None:
        """Write from polars.DataFrame into PostgreSQL/GreenPlum table."""

        self.from_rows(
            dtype_data=data_frame.iter_rows(),
            table_name=table_name,
            source=DBMetadata(
                name="polars",
                version="DataFrame",
                columns=OrderedDict(zip(
                    data_frame.columns,
                    [str(dtype) for dtype in data_frame.dtypes],
                )),
            )
        )

    def refresh(self) -> None:
        """Refresh session."""

        self.connect = Connection.connect(**self.connector._asdict())
        self.cursor = self.connect.cursor()
        self.copy_buffer.cursor = self.cursor
        self.logger.info(f"Connection to host {self.connector.host} updated.")

    def close(self) -> None:
        """Close session."""

        self.cursor.close()
        self.connect.close()
        self.logger.info(f"Connection to host {self.connector.host} closed.")
