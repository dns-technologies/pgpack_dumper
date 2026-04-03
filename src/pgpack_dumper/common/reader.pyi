from typing import Iterator, Iterable

from psycopg import Copy


class CopyReader:
    """Read from iterable Copy object."""

    copyobj: Iterable[Copy]
    iterator: Iterator[bytearray]
    bufferobj: bytearray
    first_data: bytes
    closed: bool
    total_read: int

    def __init__(
        self,
        copyobj: Iterable[Copy],
    ) -> None:
        """Class initialization."""

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
