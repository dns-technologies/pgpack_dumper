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
)

from base_dumper import (
    BaseDumper,
    WriterType,
    CompressionMethod,
    CompressionLevel,
    CSVStreamReader,
    DBMetadata,
    DebugInfo,
    DumperMode,
    DumperType,
    DumpFormat,
    IsolationLevel,
    ReaderType,
    Timeout,
    get_query_kind,
    multiquery,
    log_table,
)
from csvpack import CSVWriter
from pgpack import (
    PGCopyWriter,
    PGPackError,
    PGPackMeta,
)
from psycopg import (
    Connection,
    Copy,
    Cursor,
)

from .common import (
    CopyBuffer,
    CopyReader,
    LibSelector,
    PGConnector,
    PGPackDumperError,
    PGPackDumperReadError,
    PGPackDumperWriteBetweenError,
    PGPackDumperWriteError,
    PGPackStreamReader,
    defines,
    csvpack_meta,
    get_info,
    isolation_level,
    make_columns,
    query_template,
    statement_seconds,
)
from .version import __version__


class PGPackDumper(BaseDumper):
    """Class for read and write PGPack format."""

    connector: PGConnector
    compression_method: CompressionMethod
    logger: Logger
    timeout: int
    isolation: IsolationLevel
    mode: DumperMode
    dump_format: DumpFormat
    s3_file: bool
    application_name: str
    connect: Connection
    cursor: Cursor
    copy_buffer: CopyBuffer

    def __init__(
        self,
        connector: PGConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.ZSTD_DEFAULT,
        logger: Logger | None = None,
        timeout: int | None = None,
        isolation: IsolationLevel = IsolationLevel.committed,
        mode: DumperMode = DumperMode.PROD,
        dump_format: DumpFormat = DumpFormat.BINARY,
        s3_file: bool = False,
    ) -> None:
        """Class initialization."""

        self.dumper_version = __version__

        super().__init__(
            connector,
            compression_method,
            compression_level,
            logger,
            timeout,
            isolation,
            mode,
            dump_format,
            s3_file,
        )

        try:
            self.application_name = f"{self.__class__.__name__}/{__version__}"
            self.connect = Connection.connect(
                autocommit=True,
                application_name=self.application_name,
                **self.connector._asdict(),
            )
            self.cursor = self.connect.cursor()
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperError(error)

        version = (
            f"{self.connect.info.server_version // 10000}."
            f"{self.connect.info.server_version % 1000}"
        )
        self.isolation = isolation
        self.cursor.execute(query_template("dbname"))
        self.dbname, self.is_readonly = self.cursor.fetchone()
        self.copy_buffer = CopyBuffer(
            self.cursor,
            self.logger,
            self.dump_format.name.lower(),
            self.is_readonly,
        )

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
            if self.dump_format is DumpFormat.BINARY:
                dump_format = f"{self.dump_format.name} [{self.stream_type}]"
            else:
                dump_format = self.dump_format.name

            self.logger.info(
                "PGPackDumper additional info:\n"
                f"Version: {self.dumper_version}\n"
                f"Application name: {self.application_name}\n"
                f"Compression method: {self.compression_method.name}\n"
                f"Compression level: {self.compression_level}\n"
                f"Dump format: {dump_format}\n"
                f"Statement timeout: {self.timeout} seconds\n"
                f"Isolation level: {self.isolation.value}\n"
            )

            if self.is_readonly:
                self.logger.warning("Read-only session. Write don't work!")

    def __dbmeta(self, metadata: bytes) -> DBMetadata:
        """Generate DBMetadata from PGPack metadata."""

        pg_meta = PGPackMeta.from_bytes(metadata)
        return DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=make_columns(
                pg_meta.columns,
                pg_meta.pgtypes,
                pg_meta.pgparams,
            ),
        )

    def __read_data(
        self,
        copy_to: Iterator[Copy],
    ) -> Generator[bytes, None, None]:
        """Generate bytes from copy object."""

        for data in copy_to:
            yield bytes(data)

    @property
    def dump_format(self) -> DumpFormat:
        """Property method for get dump_format value."""

        return self._dump_format

    @dump_format.setter
    def dump_format(self, dump_format_value: DumpFormat) -> DumpFormat:
        """Property method for set dump_format value."""

        self._dump_format = dump_format_value
        self.copy_buffer.dump_format = self._dump_format.name.lower()
        return self._dump_format

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
                self.logger.info("Get query debug info.")

                if kind not in ("Delete", "Insert", "Select", "Update"):
                    start_time = time()
                    self.cursor.execute(action_data)
                    duration = round(time() - start_time, 3)
                    return self.logger.info(DebugInfo(host, kind, duration))

                query = (
                    "explain (analyze, verbose, buffers, settings, "
                    f"summary, format json)\n{action_data}"
                )

                if kind == "Insert":
                    query = f"{query}\nreturning 1"

                self.cursor.execute(query)
                explain = self.cursor.fetchone()[0]

                return self.logger.info(get_info(host, kind, explain))

            return action_data(*args, **kwargs)

    def metadata(
        self,
        query: str | None = None,
        table_name: str | None = None,
        reader_meta: bool = False,
    ) -> DBMetadata | bytes:
        """Read metadata from Server."""

        metadata = self.copy_buffer.metadata(query, table_name)

        if reader_meta:
            return metadata

        return self.__dbmeta(metadata)

    @multiquery
    def _read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None,
        table_name: str | None,
    ) -> bool:
        """Internal method read_dump for generate kwargs to decorator."""

        try:
            metadata = self.metadata(query, table_name, True)
            source = self.__dbmeta(metadata)
            destination = DBMetadata("file", fileobj.name, source.columns)
            log_table(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return

            if self.dump_format is DumpFormat.CSV:
                metadata = csvpack_meta(metadata, self.dbname, self.version)

            writer: WriterType = LibSelector[self.dump_format.name].write(
                metadata,
                fileobj,
                self.compression_method,
                self.compression_level,
                self.s3_file,
            )

            with self.copy_buffer.copy_to(query, table_name) as copy_to:
                writer.from_bytes(self.__read_data(copy_to))

            writer_size = writer.tell()
            writer.close()
            self.logger.info(f"Successfully read {writer_size} bytes.")
            self.logger.info(
                f"Read pgpack dump from {self.connector.host} done."
            )
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperReadError(error)

    def write_between(
        self,
        table_dest: str,
        table_src: str | None = None,
        query_src: str | None = None,
        dumper_src: DumperType | None = None,
    ) -> None:
        """Write stream between Servers."""

        try:
            class HiddenPGPackDumper(PGPackDumper):
                """Hidden PGPackDumper for write between on one server."""

                def __init__(self, parent: PGPackDumper) -> None:
                    self.application_name = parent.application_name
                    self.connector = parent.connector
                    self.compression_method = parent.compression_method
                    self.compression_level = parent.compression_level
                    self.mode = parent.mode
                    self._dump_format = parent.dump_format
                    self.dbname = parent.dbname
                    self.version = parent.version
                    self.dumper_version = parent.dumper_version
                    self.with_compression = parent.with_compression
                    self.is_between = parent.is_between
                    self.logger = parent.logger
                    self.is_readonly = parent.is_readonly
                    self.connect = Connection.connect(
                        autocommit=True,
                        application_name=self.application_name,
                        **self.connector._asdict(),
                    )
                    self.cursor = self.connect.cursor()
                    self.copy_buffer = CopyBuffer(
                        self.cursor,
                        self.logger,
                        self._dump_format.name.lower(),
                        self.is_readonly,
                    )
                    self.timeout = parent.timeout
                    self.isolation = parent.isolation

            if not dumper_src:
                self.logger.info(
                    f"Set new connection for host {self.connector.host}.",
                )
                dumper_src = HiddenPGPackDumper(self)
                self.logger.info(
                    f"New connection for host {self.connector.host} success.",
                )

            super().write_between(
                table_dest,
                table_src,
                query_src,
                dumper_src,
            )
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperWriteBetweenError(error)
        finally:
            if dumper_src and isinstance(dumper_src, HiddenPGPackDumper):
                dumper_src.close()

    @multiquery
    def _to_reader(
        self,
        query: str | None,
        table_name: str | None,
        metadata: bytes | None = None,
    ) -> ReaderType | DBMetadata:
        """Internal method to_reader for generate kwargs to decorator."""

        try:
            if not metadata:
                metadata = self.metadata(query, table_name, True)

            db_metadata = self.__dbmeta(metadata)
            fileobj = self._to_fileobj(query, table_name, db_metadata)

            if isinstance(fileobj, DBMetadata):
                return fileobj

            if self.dump_format is DumpFormat.CSV:
                return CSVStreamReader(fileobj, db_metadata)

            return PGPackStreamReader(
                fileobj,
                metadata,
                self.dbname,
                self.version,
            )
        except PGPackError as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperReadError(error)

    def _to_fileobj(
        self,
        query: str | None,
        table_name: str | None,
        metadata: DBMetadata | None = None,
    ) -> BufferedReader | DBMetadata:
        """Internal method to_fileobj for generate kwargs to decorator."""

        if self.mode is DumperMode.TEST and not self.is_between:

            if not metadata:
                metadata = self.metadata(query, table_name)

            log_table(self.logger, self.mode, metadata)
            return metadata

        copyobj = self.copy_buffer.copy_to(query, table_name)
        return CopyReader(copyobj)

    def write_dump(
        self,
        fileobj: BufferedReader,
        table_name: str,
    ) -> None:
        """Write CSVPack/PGPack dump into PostgreSQL/GreenPlum."""

        try:
            reader: ReaderType = LibSelector[self.dump_format.name].read(
                fileobj,
            )
            source = DBMetadata(
                name="file",
                version=fileobj.name,
                columns={
                    column: dtype
                    for column, dtype in zip(reader.columns, reader.dtypes)
                },
            )
            pg_meta = PGPackMeta.from_bytes(self.metadata(
                table_name=table_name,
                reader_meta=True,
            ))
            destination = DBMetadata(
                name=self.dbname,
                version=self.version,
                columns=make_columns(
                    pg_meta.columns,
                    pg_meta.pgtypes,
                    pg_meta.pgparams,
                ),
            )
            log_table(self.logger, self.mode, source, destination)

            if self.mode is DumperMode.TEST:
                return reader.close()

            collect()
            self.copy_buffer.copy_from(reader.to_bytes(), table_name)
            self.connect.commit()
            reader.close()
        except Exception as error:
            self.logger.error(f"{error.__class__.__name__}: {error}")
            raise PGPackDumperWriteError(error)

    def from_rows(
        self,
        dtype_data: Iterable[Any],
        table_name: str,
        source: DBMetadata | object | None = None,
    ) -> None:
        """Write from iterable object into PostgreSQL/GreenPlum table."""

        if not source:
            source, dtype_data = self._db_meta_from_iter(dtype_data)

        metadata = self.metadata(table_name=table_name, reader_meta=True)
        destination = self.__dbmeta(metadata)

        if self.dump_format is DumpFormat.BINARY:
            pg_meta = PGPackMeta.from_bytes(metadata)
            writer = PGCopyWriter(pg_meta.pgcopy_metadata)
        elif self.dump_format is DumpFormat.CSV:
            csv_meta = csvpack_meta(
                metadata,
                self.dbname,
                self.version,
            )
            writer = CSVWriter(*csv_meta[4:])
        else:
            error = f"Unknown dump format {self.dump_format}"
            self.logger.error(f"PGPackDumperWriteError: {error}")
            raise PGPackDumperWriteError(error)

        bytes_data = writer.from_rows(dtype_data)
        self.from_bytes(bytes_data, table_name, source, destination)
        collect()

    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
        table_name: str,
        source: DBMetadata | object | None = None,
        destination: DBMetadata | None = None,
    ) -> None:
        """Write from iterable bytes into Server object."""

        if not source:
            raise PGPackDumperWriteError("Source metadata not define.")

        if not isinstance(source, DBMetadata):
            source = self.__dbmeta(source)

        if not destination:
            destination = self.metadata(table_name=table_name)

        log_table(self.logger, self.mode, source, destination)

        if self.mode is DumperMode.TEST:
            return

        self.copy_buffer.copy_from(bytes_data, table_name)
        self.connect.commit()

    def refresh(self) -> None:
        """Refresh session."""

        self.connect = Connection.connect(
            application_name=self.application_name,
            **self.connector._asdict(),
        )
        self.cursor = self.connect.cursor()
        self.copy_buffer.cursor = self.cursor
        self.logger.info(f"Connection to host {self.connector.host} updated.")

    def close(self) -> None:
        """Close session."""

        self.cursor.close()
        self.connect.close()
        self.logger.info(f"Connection to host {self.connector.host} closed.")
