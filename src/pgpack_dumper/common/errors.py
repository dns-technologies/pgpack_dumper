from base_dumper import (
    BaseDumperError,
    BaseDumperTypeError,
    BaseDumperValueError,
)

class CopyBufferError(BaseDumperError):
    """CopyBuffer base error."""


class CopyBufferObjectError(CopyBufferError, BaseDumperTypeError):
    """Destination object not support."""


class CopyBufferTableNotDefined(CopyBufferError, BaseDumperValueError):
    """Destination table not defined."""


class PGPackDumperError(BaseDumperError):
    """PGPackDumper base error."""


class PGPackDumperReadError(PGPackDumperError):
    """PGPackDumper read error."""


class PGPackDumperWriteError(PGPackDumperError):
    """PGPackDumper write error."""


class PGPackDumperWriteBetweenError(PGPackDumperWriteError):
    """PGPackDumper write between error."""
