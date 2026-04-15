from logging import Logger
from time import time
from typing import (
    BinaryIO,
    Generator,
    Iterator,
)

from psycopg import (
    Copy,
    Cursor,
)

from .errors import (
    CopyBufferError,
    CopyBufferObjectError,
    CopyBufferTableNotDefined,
)
from .metadata import read_metadata
from .query import (
    query_template,
    search_object,
)
from .structs import PGObject


class CopyBuffer:
    """Class for work with Postgres/Greenplum server."""

    cursor: Cursor
    logger: Logger
    dump_format: str | None
    is_readonly: bool

    def __init__(
        self,
        cursor: Cursor,
        logger: Logger,
        dump_format: str,
        is_readonly: bool,
    ) -> None:
        """Class initialization."""

        self.cursor = cursor
        self.logger = logger
        self.dump_format = dump_format
        self.is_readonly = is_readonly

    def metadata(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> bytes:
        """Get metadata as bytes."""

        host = self.cursor.connection.info.host
        self.logger.info(f"Start read metadata from host {host}.")
        metadata = read_metadata(
            self.cursor,
            query,
            table_name,
            self.is_readonly,
        )
        self.logger.info(f"Read metadata from host {host} done.")
        return metadata

    def object_writer(
        self,
        source: Iterator[Copy | bytes],
        destination: BinaryIO,
    ) -> int:
        """Write data into binary object."""

        start = time()
        size = 0

        for bytes_data in source:
            size += len(bytes_data)
            destination.write(bytes_data)
            del bytes_data

        duration = round(time() - start, 3)
        self.logger.info(f"Duration time is {duration} seconds.")
        self.logger.info(f"Successfully sending {size} bytes.")
        return size

    def raise_if_not_stream(self) -> None:
        """Raise error if not stream_type."""

        if not self.dump_format:
            error_msg = "Stream type not defined."
            self.logger.error(f"CopyBufferError: {error_msg}")
            raise CopyBufferError(error_msg)

    def copy_to(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> Iterator[Copy]:
        """Get copy object from PostgreSQL."""

        if not query and not table_name:
            error_msg = "Query or table not defined."
            self.logger.error(f"CopyBufferTableNotDefined: {error_msg}")
            raise CopyBufferTableNotDefined(error_msg)

        self.raise_if_not_stream()
        host = self.cursor.connection.info.host

        if not query:
            self.logger.info(
                f"Start read from {host}.{table_name}.".replace('"', ""),
            )
            self.cursor.execute(query_template("relkind").format(
                table_name=table_name,
            ))
            relkind = self.cursor.fetchone()[0]
            pg_object = PGObject[relkind]

            if not pg_object.is_readable:
                error_msg = f"Read from {pg_object} not support."
                self.logger.error(f"CopyBufferObjectError: {error_msg}")
                raise CopyBufferObjectError(error_msg)

            self.logger.info(f"Use method read from {pg_object}.")

            if not pg_object.is_readobject:
                table_name = f"(select * from {table_name})"

        elif query:
            self.logger.info(f"Start read query from {host}.")
            self.logger.info("Use method read from select.")
            table_name = f"({query}\n)"

        return self.cursor.copy(
            query_template("copy_to").format(
                table_name=table_name,
                dump_format=self.dump_format,
            )
        )

    def copy_from(
        self,
        copyobj: Iterator[bytes],
        table_name: str | None = None,
    ) -> None:
        """Write PGCopy dump into PostgreSQL."""

        if not table_name:
            error_msg = "Table not defined."
            self.logger.error(f"CopyBufferTableNotDefined: {error_msg}")
            raise CopyBufferTableNotDefined(error_msg)

        self.raise_if_not_stream()
        host = self.cursor.connection.info.host
        self.logger.info(
            f"Start write into {host}.{table_name}.".replace('"', ""),
        )

        with self.cursor.copy(
            query_template("copy_from").format(
                table_name=table_name,
                dump_format=self.dump_format,
            )
        ) as cp:
            self.object_writer(copyobj, cp)

        self.logger.info(
            f"Write into {host}.{table_name} done.".replace('"', ""),
        )

    def copy_between(
        self,
        copy_buffer: "CopyBuffer",
        table_dest: str | None = None,
        table_src: str | None = None,
        query_src: str | None = None,
    ) -> None:
        """Write from PostgreSQL into PostgreSQL."""

        with copy_buffer.copy_to(query_src, table_src) as copy_to:
            destination_host = self.cursor.connection.info.host
            source_host = copy_buffer.cursor.connection.info.host
            source_object = search_object(table_src, query_src)
            message = (
                f"Copy {source_object} from {source_host} into "
                f"{destination_host}.{table_dest} started."
            ).replace('"', "")
            self.logger.info(message)

            with self.cursor.copy(
                query_template("copy_from").format(
                    table_name=table_dest,
                    dump_format=self.dump_format,
                )
            ) as copy_from:
                self.object_writer(copy_from, copy_to)

            message = (
                f"Copy {source_object} from {source_host}"
                f"into {destination_host}.{table_dest} done."
            ).replace('"', "")
            self.logger.info(message)

    def copy_reader(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> Generator[bytes, None, None]:
        """Read bytes from copy object."""

        host = self.cursor.connection.info.host
        source = search_object(table_name, query)

        with self.copy_to() as copy_object:
            for data in copy_object:
                yield bytes(data)

        self.logger.info(f"Read {source} from {host} done.".replace('"', ""))
