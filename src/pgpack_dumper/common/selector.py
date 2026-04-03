from enum import Enum
from typing import NamedTuple

from csvpack import (
    CSVPackReader,
    CSVPackWriter,
)
from pgpack import (
    PGPackReader,
    PGPackWriter,
)


class LibVariant(NamedTuple):
    """Current reader/writer for selected method."""

    read: CSVPackReader | PGPackReader
    write: CSVPackWriter | PGPackWriter


class LibSelector(LibVariant, Enum):
    """Enum for LibVariant."""

    CSV = LibVariant(CSVPackReader, CSVPackWriter)
    BINARY = LibVariant(PGPackReader, PGPackWriter)
