class CSVPatcherError(Exception):
    """Base CSVPatcher error."""


class CSVPatcherValueError(CSVPatcherError, ValueError):
    """CSVPatcher value error."""


class CSVPatcherTypeError(CSVPatcherError, TypeError):
    """CSVPatcher type error."""
