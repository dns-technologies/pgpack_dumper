from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from types import MethodType
from typing import (
    Any,
    Iterable,
    Union
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
    Cursor,
)
from pandas import DataFrame as PdFrame
from polars import DataFrame as PlFrame
from sqlparse import format as sql_format

from .common import (
    CopyBuffer,
    DumperLogger,
    PGConnector,
    PGPackDumperError,
    PGPackDumperReadError,
    PGPackDumperWriteError,
    PGPackDumperWriteBetweenError,
    StreamReader,
    chunk_query,
    query_template,
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
            self.version = f"{self.version}|greenplum {gpversion}"

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
            cursor: Cursor = kwargs.get("dumper_src", self).cursor
            query: str = kwargs.get("query_src") or kwargs.get("query")
            part: int = 1
            first_part, second_part = chunk_query(self.query_formatter(query))
            total_prts = len(sum((first_part, second_part), [])) or 1

            if first_part:
                self.logger.info("Multiquery detected.")

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

        try:
            self.copy_buffer.query = query
            self.copy_buffer.table_name = table_name
            pgpack = PGPackWriter(
                fileobj,
                self.copy_buffer.metadata,
                self.compression_method,
            )

            with self.copy_buffer.copy_to() as copy_to:
                pgpack.from_bytes(bytes(data) for data in copy_to)

            size = pgpack.tell()
            pgpack.close()
            self.logger.info(f"Successfully read {size} bytes.")
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
            elif dumper_src.__class__ is PGPackDumper:
                source_copy_buffer = dumper_src.copy_buffer
                source_copy_buffer.table_name = table_src
                source_copy_buffer.query = query_src
            else:
                reader = dumper_src.to_reader(
                    query=query_src,
                    table_name=table_src,
                )
                dtype_data = reader.to_rows()
                self.from_rows(
                    dtype_data=dtype_data,
                    table_name=table_dest,
                )
                size = reader.tell()
                self.logger.info(f"Successfully sending {size} bytes.")
                return reader.close()

            self.copy_buffer.table_name = table_dest
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
        return StreamReader(
            self.copy_buffer.metadata,
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
            pgpack = PGPackReader(fileobj)
            self.copy_buffer.table_name = table_name
            self.copy_buffer.copy_from(pgpack.to_bytes())
            self.connect.commit()
            size = pgpack.tell()
            self.logger.info(f"Successfully sending {size} bytes.")
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
    ) -> None:
        """Write from python iterable object
        into PostgreSQL/GreenPlum table."""

        self.copy_buffer.table_name = table_name
        _, pgtypes, _ = metadata_reader(self.copy_buffer.metadata)
        writer = PGCopyWriter(None, pgtypes)
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
