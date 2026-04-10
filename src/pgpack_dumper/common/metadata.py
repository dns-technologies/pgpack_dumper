from json import dumps

from base_dumper import random_name
from csvpack import CSVPackMeta
from pgpack import PGPackMeta
from pgpack.common import compile_pgtype
from psycopg import Cursor

from .query import query_template


def read_metadata(
    cursor: Cursor,
    query: str | None = None,
    table_name: str | None = None,
    is_readonly: bool = False,
) -> bytes:
    """Read metadata for query or table."""

    if not query and not table_name:
        raise ValueError("No object defined.")

    if query:
        query = query.strip("; \t\n\r")

        if "limit" in query.lower():
            query = f"select * from ({query}\n) as {random_name()}"

        if is_readonly:
            cursor.execute(f"{query} limit 0")
            metadata = [
                {column.name: {
                    "oid": column.type_code,
                    "length": column.internal_size or
                    column.precision or
                    column.display_size or -1,
                    "scale": column.scale or 0,
                    "nested": int("[]" in str(column)),
                }}
                for column in cursor.description
            ]

            return dumps(
                metadata,
                ensure_ascii=False,
            ).encode("utf-8")

        session_name = random_name()
        prepare_name = f"{session_name}_prepare"
        table_name = f"{session_name}_temp"
        cursor.execute(query_template("prepare").format(
            prepare_name=prepare_name,
            query=query,
            table_name=table_name,
        ))

    cursor.execute(query_template("attributes").format(
        table_name=table_name,
    ))

    metadata: bytes = cursor.fetchone()[0]

    if query:
        cursor.execute(f"drop table if exists {table_name};")

    return metadata


def csvpack_meta(
    metadata: bytes,
    source: str,
    version: str,
) -> CSVPackMeta:
    """Generate CSVPackMeta object from PGPck metadata."""

    pgpack_meta = PGPackMeta.from_bytes(metadata)
    return CSVPackMeta.from_params(
        source,
        version,
        pgpack_meta.columns,
        [
            compile_pgtype(pgtype, param)
            for pgtype, param in zip(
                pgpack_meta.pgtypes,
                pgpack_meta.pgparams,
            )
        ],
    )
