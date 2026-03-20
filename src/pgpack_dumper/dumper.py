from collections.abc import Generator
from gc import collect
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from time import time
from types import MethodType
from typing import (
    Any,
    Iterable,
    Iterator,
    Union,
)

from base_dumper import (
    BaseDumper,
    CompressionMethod,
    CompressionLevel,
    DBMetadata,
    DebugInfo,
    DumperMode,
    DumpFormat,
    IsolationLevel,
    Timeout,
    multiquery,
    log_diagram,
)
from pgcopylib import PGCopyWriter
from pgpack import (
    PGPackError,
    PGPackReader,
    PGPackWriter,
    metadata_reader,
)
from psycopg import (
    Connection,
    Copy,
    Cursor,
)

from .common import (
    CopyBuffer,
    PGConnector,
    PGPackDumperError,
    PGPackDumperReadError,
    PGPackDumperWriteBetweenError,
    PGPackDumperWriteError,
    StreamReader,
    defines,
    get_info,
    get_query_kind,
    isolation_level,
    make_columns,
    query_template,
    statement_seconds,
)
from .version import __version__


class PGPackDumper(BaseDumper):
    """Class for read and write PGPack format."""

    def __init__(
        self,
        connector: PGConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.ZSTD_DEFAULT,
        logger: Logger | None = None,
        timeout: int | None = None,
        isolation: IsolationLevel = IsolationLevel.committed,
        mode: DumperMode = DumperMode.PROD,
        dump_format: DumpFormat = DumpFormat.RAW,
    ) -> None:
        """Class initialization."""

        self.__version__ = __version__

        super().__init__(
            connector,
            compression_method,
            compression_level,
            logger,
            timeout,
            isolation,
            mode,
            dump_format,
        )

        try:
            self.application_name = f"{self.__class__.__name__}/{__version__}"
            self.connect: Connection = Connection.connect(
                application_name=self.application_name,
                **self.connector._asdict(),
            )
            self.cursor: Cursor = self.connect.cursor()
            self.copy_buffer: CopyBuffer = CopyBuffer(self.cursor, self.logger)
            self._dbmeta: DBMetadata | None = None
            self._size = 0
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperError(error)

        version = (
            f"{self.connect.info.server_version // 10000}."
            f"{self.connect.info.server_version % 1000}"
        )
        self.stream_type = "pgcopy"
        self.isolation = isolation
        self.cursor.execute(query_template("dbname"))
        self.dbname, self.is_readonly = self.cursor.fetchone()
        self.copy_buffer.is_readonly = self.is_readonly

        if timeout is None:
            if self.dbname == "greenplum":
                timeout = Timeout.GREENPLUM_DEFAULT_TIMEOUT
            elif self.dbname == "postgres":
                timeout = Timeout.POSTGRES_DEFAULT_TIMEOUT

        self.timeout = timeout

        if self.dbname == "greenplum":
            self.cursor.execute(query_template("gpversion"))
            gpversion = self.cursor.fetchone()[0]
            self.version = f"{gpversion} (postgres {version})"
        else:
            self.version = version

        self.logger.info(
            f"PGPackDumper initialized for host {self.connector.host}"
            f"[{self.dbname} {self.version}]"
        )

        if self.mode is not DumperMode.PROD:
            self.logger.info(
                "PGPackDumper additional info:\n"
                f"Version: {self.__version__}\n"
                f"Application name: {self.application_name}\n"
                f"Compression method: {self.compression_method.name}\n"
                f"Compression level: {self.compression_level}\n"
                f"Dump format: {self.dump_format.name}\n"
                f"Statement timeout: {self.timeout} seconds\n"
                f"Isolation level: {self.isolation.value}\n"
            )

            if self.is_readonly:
                self.logger.warning("Read-only session. Write don't work!")

    @property
    def timeout(self) -> int:
        """Property method for get statement_timeout."""

        return self._timeout

    @timeout.setter
    def timeout(self, timeout_value: int) -> int:
        """Property method for set statement_timeout."""

        set_value = defines.SET_TIMEOUT.format(timeout_value)
        self.cursor.execute(set_value)
        self.connect.commit()
        self.cursor.execute(defines.GET_TIMEOUT)
        self._timeout = statement_seconds(self.cursor.fetchone()[0])
        return self._timeout

    @property
    def isolation(self) -> IsolationLevel:
        """Property method for get current
        server transaction isolation level."""

        return self._isolation

    @isolation.setter
    def isolation(self, isolation_value: IsolationLevel) -> IsolationLevel:
        """Property method for set current
        server transaction isolation level."""

        set_value = defines.SET_ISOLATION_LEVEL.format(isolation_value.value)
        self.cursor.execute(set_value)
        self.connect.commit()
        self.cursor.execute(defines.GET_ISOLATION_LEVEL)
        self._isolation = isolation_level(self.cursor.fetchone()[0])
        return self._isolation

    def mode_action(
        self,
        action_data: str | MethodType | None = None,
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> None:
        """DumperMode.DEBUG or DumperMode.TEST action."""

        if action_data:
            if isinstance(action_data, str):
                if self.mode is DumperMode.PROD:
                    return self.cursor.execute(action_data)

                host = self.connector.host
                kind = get_query_kind(action_data)

                if kind in ("Create", "Drop"):
                    start_time = time()
                    self.cursor.execute(action_data)
                    self.logger.info("Get query debug info.")
                    duration = round(time() - start_time, 3)
                    return self.logger.info(DebugInfo(host, kind, duration))

                query = (
                    "explain (analyze, verbose, buffers, settings, "
                    f"summary, format json)\n{action_data}"
                )

                if kind == "Insert":
                    query = f"{query}\nreturning 1"

                self.cursor.execute(query)
                self.logger.info("Get query debug info.")
                explain = self.cursor.fetchone()[0]

                return self.logger.info(get_info(
                    host,
                    kind,
                    explain,
                ))

            return action_data(*args, **kwargs)

    @multiquery
    def _read_dump(
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

            log_diagram(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return

            pgpack = PGPackWriter(
                fileobj,
                metadata,
                self.compression_method,
                self.compression_level,
                self.dump_format is DumpFormat.S3,
            )

            with self.copy_buffer.copy_to() as copy_to:
                pgpack.from_bytes(__read_data(copy_to))

            pgpack.close()
            self.logger.info(f"Successfully read {self._size} bytes.")
            self.logger.info(
                f"Read pgpack dump from {self.connector.host} done."
            )
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperReadError(error)

    @multiquery
    def _write_between(
        self,
        table_dest: str,
        table_src: str | None,
        query_src: str | None,
        dumper_src: Union["PGPackDumper", object],
    ) -> bool:
        """Internal method write_between for generate kwargs to decorator."""

        try:
            if not dumper_src or dumper_src is self:
                src_dbname = self.dbname
                src_version = self.version
                source_copy_buffer = self.copy_buffer
                self.logger.info(
                    f"Set new connection for host {self.connector.host}."
                )
                connect = Connection.connect(**self.connector._asdict())
                self.copy_buffer = CopyBuffer(
                    connect.cursor(),
                    self.logger,
                    query_src,
                    table_src,
                )
            elif dumper_src.__class__ is PGPackDumper:
                source_copy_buffer = dumper_src.copy_buffer
                src_dbname = dumper_src.dbname
                src_version = dumper_src.version
            else:
                if self.mode is DumperMode.TEST:
                    return self.from_rows(
                        dtype_data=None,
                        table_name=table_dest,
                        source=dumper_src._dbmeta,
                    )

                reader = dumper_src.to_reader(
                    query=query_src,
                    table_name=table_src,
                )
                self.from_rows(
                    dtype_data=reader.to_rows(),
                    table_name=table_dest,
                    source=dumper_src._dbmeta,
                )
                size = reader.tell()
                self.logger.info(f"Successfully sending {size} bytes.")
                return reader.close()

            source_copy_buffer.table_name = table_src
            source_copy_buffer.query = query_src
            source = DBMetadata(
                name=src_dbname,
                version=src_version,
                columns=make_columns(
                    *metadata_reader(source_copy_buffer.metadata),
                ),
            )
            self.copy_buffer.table_name = table_dest
            self.copy_buffer.query = None
            destination = DBMetadata(
                name=self.dbname,
                version=self.version,
                columns=make_columns(
                    *metadata_reader(self.copy_buffer.metadata),
                ),
            )
            log_diagram(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return

            self.copy_buffer.copy_between(source_copy_buffer)
            self.connect.commit()
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperWriteBetweenError(error)

    @multiquery
    def _to_reader(
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

        try:
            if self.mode is DumperMode.TEST:
                log_diagram(self.logger, self.mode, self._dbmeta)
                return self._dbmeta

            return StreamReader(
                metadata,
                self.copy_buffer.copy_to(),
            )
        except PGPackError as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperReadError(error)

    def write_dump(
        self,
        fileobj: BufferedReader,
        table_name: str,
    ) -> None:
        """Write PGPack dump into PostgreSQL/GreenPlum."""

        try:
            self.copy_buffer.table_name = table_name
            self.copy_buffer.query = None
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

            log_diagram(self.logger, self.mode, source, destination)
            collect()

            if self.mode is DumperMode.TEST:
                return pgpack.close()

            self.copy_buffer.copy_from(pgpack.to_bytes())
            self.connect.commit()
            pgpack.close()
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperWriteError(error)

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
        self.copy_buffer.query = None
        columns, pgtypes, pgparam = metadata_reader(self.copy_buffer.metadata)
        destination = DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=make_columns(
                list_columns=columns,
                pgtypes=pgtypes,
                pgparam=pgparam,
            ),
        )

        log_diagram(self.logger, self.mode, source, destination)

        if self.mode is DumperMode.TEST:
            return

        writer = PGCopyWriter(None, pgtypes)
        collect()
        self.copy_buffer.copy_from(writer.from_rows(dtype_data))
        self.connect.commit()

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
