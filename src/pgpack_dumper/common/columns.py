from collections import OrderedDict

from pgpack import PGOid
from pgpack.common import (
    PGParam,
    compile_pgtype,
)


def make_columns(
    list_columns: list[str],
    pgtypes: list[PGOid],
    pgparam: list[PGParam],
) -> OrderedDict[str, str]:
    """Make DBMetadata.columns dictionary."""

    columns = OrderedDict()

    for column, pgtype, param in zip(
        list_columns,
        pgtypes,
        pgparam,
    ):
        columns[column] = compile_pgtype(pgtype, param)

    return columns
