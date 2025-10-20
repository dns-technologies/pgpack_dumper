from collections import OrderedDict

from pgcopylib import PGOid
from pgpack.common import PGParam


def make_columns(
    list_columns: list[str],
    pgtypes: list[PGOid],
    pgparam: list[PGParam],
) -> OrderedDict[str, str]:
    """Make DBMetadata.columns dictionary."""

    columns = OrderedDict()

    for col_name, pgtype, param in zip(
        list_columns,
        pgtypes,
        pgparam,
    ):
        col_type = pgtype.name

        if pgtype is PGOid.bpchar:
            col_type = f"{col_type}({param.length})"
        elif pgtype is PGOid.numeric:
            col_type = f"{col_type}({param.length}, {param.scale})"

        columns[col_name] = col_type

    return columns
