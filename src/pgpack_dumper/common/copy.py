from logging import Logger
from typing import (
    Generator,
    Iterator,
)

from psycopg import (
    Copy,
    Cursor,
)

from .errors import (
    CopyBufferObjectError,
    CopyBufferTableNotDefined,
)
from .query import (
    query_template,
    search_object,
)
from .structs import PGObject
from .metadata import read_metadata


class CopyBuffer:

    def __init__(
        self,
        cursor: Cursor,
        logger: Logger,
        query: str | None = None,
        table_name: str | None = None,
    ) -> None:
        """Class initialization."""

        self.cursor = cursor
        self.logger = logger
        self.query = query
        self.table_name = table_name

    @property
    def metadata(self) -> bytes:
        """Get metadata as bytes."""

        host = self.cursor.connection.info.host
        self.logger.info(f"Start read metadata from host {host}.")
        metadata = read_metadata(
            self.cursor,
            self.query,
            self.table_name,
        )
        self.logger.info(f"Read metadata from host {host} done.")
        return metadata

    def copy_to(self) -> Iterator[Copy]:
        """Get copy object from PostgreSQL."""

        if not self.query and not self.table_name:
            error_msg = "Query or table not defined."
            self.logger.error(f"CopyBufferTableNotDefined: {error_msg}")
            raise CopyBufferTableNotDefined(error_msg)

        host = self.cursor.connection.info.host

        if not self.query:
            self.logger.info(f"Start read from {host}.{self.table_name}.")
            self.cursor.execute(query_template("relkind").format(
                table_name=self.table_name,
            ))
            relkind = self.cursor.fetchone()[0]
            pg_object = PGObject[relkind]
            if not pg_object.is_readable:
                error_msg = f"Read from {pg_object} not support."
                self.logger.error(f"CopyBufferObjectError: {error_msg}")
                raise CopyBufferObjectError(error_msg)
            self.logger.info(f"Use method read from {pg_object}.")
            if not pg_object.is_readobject:
                self.table_name = f"(select * from {self.table_name})"
        elif self.query:
            self.logger.info(f"Start read query from {host}.")
            self.logger.info("Use method read from select.")
            self.table_name = f"({self.query})"

        return self.cursor.copy(
            query_template("copy_to").format(table_name=self.table_name)
        )

    def copy_from(
        self,
        copyobj: Iterator[bytes],
    ) -> None:
        """Write PGCopy dump into PostgreSQL."""

        if not self.table_name:
            error_msg = "Table not defined."
            self.logger.error(f"CopyBufferTableNotDefined: {error_msg}")
            raise CopyBufferTableNotDefined(error_msg)

        host = self.cursor.connection.info.host
        size = 0
        self.logger.info(f"Start write into {host}.{self.table_name}.")

        with self.cursor.copy(
            query_template("copy_from").format(table_name=self.table_name)
        ) as cp:
            for bytes_data in copyobj:
                size += len(bytes_data)
                cp.write(bytes_data)

        self.logger.info(f"Successfully sending {size} bytes.")
        self.logger.info(f"Write into {host}.{self.table_name} done.")

    def copy_between(
        self,
        copy_buffer: "CopyBuffer",
    ) -> None:
        """Write from PostgreSQL into PostgreSQL."""

        with copy_buffer.copy_to() as copy_to:
            destination_host = self.cursor.connection.info.host
            source_host = copy_buffer.cursor.connection.info.host
            source_object = search_object(
                copy_buffer.table_name,
                copy_buffer.query,
            )
            size = 0
            self.logger.info(
                f"Copy {source_object} from {source_host} into "
                f"{destination_host}.{self.table_name} started."
            )

            with self.cursor.copy(
                query_template("copy_from").format(table_name=self.table_name)
            ) as copy_from:
                for data in copy_to:
                    size += len(data)
                    copy_from.write(data)

            self.logger.info(f"Successfully sending {size} bytes.")
            self.logger.info(
                f"Copy {source_object} from {source_host}"
                f"into {destination_host}.{self.table_name} done."
            )

    def copy_reader(self) -> Generator[bytes, None, None]:
        """Read bytes from copy object."""

        host = self.cursor.connection.info.host
        source = search_object(
            self.table_name,
            self.query,
        )

        with self.copy_to() as copy_object:
            for data in copy_object:
                yield bytes(data)

        self.logger.info(f"Read {source} from {host} done.")
