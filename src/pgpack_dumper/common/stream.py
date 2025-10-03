from typing import (
    Iterable,
    NoReturn
)

from pgcopylib import PGCopyReader
from pgpack import (
    PGPackReader,
    metadata_reader,
)
from psycopg import Copy

from .reader import CopyReader


class StreamReader(PGPackReader):
    """Class for stream read from PostgreSQL/GreenPlum."""

    def __init__(
        self,
        metadata: bytes,
        copyobj: Iterable[Copy],
    ) -> None:
        """Class initialization."""

        self.metadata = metadata
        self.copyobj = CopyReader(copyobj)
        (
            self.columns,
            self.pgtypes,
            self.pgparam,
        ) = metadata_reader(self.metadata)
        self.pgcopy = PGCopyReader(
            self.copyobj,
            self.pgtypes,
        )

    def __str__(self) -> str:
        """String representation of PGPackReader."""

        def to_col(text: str) -> str:
            """Format string element."""

            text = text[:14] + "…" if len(text) > 15 else text
            return f" {text: <15} "

        empty_line = (
            "│-----------------+-----------------│"
        )
        end_line = (
            "└─────────────────┴─────────────────┘"
        )
        _str = [
            "<PostgreSQL/GreenPlum stream reader>",
            "┌─────────────────┬─────────────────┐",
            "│ Column Name     │ PostgreSQL Type │",
            "╞═════════════════╪═════════════════╡",
        ]

        for column, pgtype in zip(self.columns, self.pgtypes):
            _str.append(
                f"│{to_col(column)}│{to_col(pgtype.name)}│",
            )
            _str.append(empty_line)

        _str[-1] = end_line
        return "\n".join(_str) + f"""
Total columns: {len(self.columns)}
Readed rows: {self.pgcopy.num_rows}
"""

    def to_bytes(self) -> NoReturn:
        """Get raw unpacked pgcopy data."""

        raise NotImplementedError("Don't support in stream mode.")

    def close(self) -> None:
        """Close stream object."""

        self.copyobj.close()
