from json import dumps

from base_dumper import random_name
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
        raise ValueError()

    if query:

        query = query.strip().strip(";")

        if "limit" in query.lower():
            query = f"select * from ({query}\n) as {random_name()}"

        if is_readonly:
            cursor.execute(f"{query} limit 0")
            metadata = [
                [
                    column_number,
                    [
                        column.name,
                        column.type_code,
                        column.internal_size or
                        column.precision or
                        column.display_size or -1,
                        column.scale or 0,
                        int("[]" in str(column)),
                    ]
                ]
                for column_number, column in
                enumerate(cursor.description, 1)
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
