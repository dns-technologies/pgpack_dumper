from collections.abc import Generator

from pgpack import (
    PGCopyReader,
    PGOid,
    PGPackMeta,
    PGPackReader,
    PGParam,
)
from pgpack.common import (
    Size,
    table_repr,
)
from polars import Object

from .reader import CopyReader


class PGPackStreamReader(PGPackReader):
    """Class for manipulate uncompressed stream csv object."""

    fileobj: CopyReader
    metadata: PGPackMeta
    dbname: str
    version: str
    columns: list[str]
    pgtypes: list[PGOid]
    pgparam: list[PGParam]
    pgcopy: PGCopyReader | None
    schema_overrides: dict[str, Object]

    def __init__(
        self,
        fileobj: CopyReader,
        metadata: bytes,
        dbname: str,
        version: str,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.dbname = dbname
        self.version = version
        self.metadata = PGPackMeta.from_bytes(metadata)
        self.columns = self.metadata.columns
        self.pgtypes = self.metadata.pgtypes
        self.pgparam = self.metadata.pgparams

        try:
            self.pgcopy = PGCopyReader(
                self.fileobj,
                self.pgtypes,
            )
        except IndexError:
            self.pgcopy = None

        self.schema_overrides = {
            column: Object
            for column, pgtype in zip(self.columns, self.pgtypes)
            if pgtype in (
                PGOid._uuid,
                PGOid._json,
                PGOid._jsonb,
                PGOid._inet,
                PGOid._cidr,
                PGOid._tsquery,
                PGOid._tsvector,
            )
        }

    def to_bytes(self) -> Generator[bytes, None, None]:
        """Get raw stream data."""

        if self.fileobj.tell() <= Size.HEADER_LENS + Size.PGDATA_PROMPT:
            yield self.fileobj.first_data[:self.fileobj.tell()]

        while chunk := self.fileobj.read(Size.CHUNK_SIZE):
            yield chunk

    def tell(self) -> int:
        """Return current position."""

        return self.fileobj.tell()

    def __repr__(self) -> str:
        """String representation of CSVPackReader."""

        return table_repr(
            self.columns,
            self.dtypes,
            "<PostgreSQL/GreenPlum stream reader>",
            [
                f"Total columns: {len(self.columns)}",
                f"Readed rows: {self.pgcopy.num_rows if self.pgcopy else 0}",
                f"Source: {self.dbname}",
                f"Version: {self.version}",
            ],
        )
