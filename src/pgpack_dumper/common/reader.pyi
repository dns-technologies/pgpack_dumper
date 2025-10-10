from typing import Iterator, Iterable

from psycopg import Copy


class CopyReader:
    """Read from iterable Copy object."""

    def __init__(
        self,
        copyobj: Iterable[Copy],
    ) -> None:
        """Class initialization."""

        self.copyobj: Iterable[Copy]
        self.iterator: Iterator[bytearray]
        self.bufferobj: bytearray
        self.closed: bool
        self.total_read: int
        ...

    def read(self, size: int) -> bytes:
        """Read from copy."""

        ...

    def tell(self) -> int:
        """Return the current stream position."""

        ...

    def close(self) -> None:
        """Close CopyReader."""

        ...
