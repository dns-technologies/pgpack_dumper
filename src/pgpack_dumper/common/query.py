from pathlib import Path
from random import randbytes
from re import (
    match,
    split,
)

pattern = r"\(select \* from (.*)\)|(.*)"


def search_object(table: str, query: str = "") -> str:
    """Return current string for object."""

    if query:
        return "query"

    return match(pattern, table).group(1) or table


def random_name() -> str:
    """Generate random name for prepare and temp table."""

    return f"session_{randbytes(8).hex()}"  # noqa: S311


def query_path() -> str:
    """Path for queryes."""

    return f"{Path(__file__).parent.absolute()}/queryes/{{}}.sql"


def query_template(query_name: str) -> str:
    """Get query template for his name."""

    path = query_path().format(query_name)

    with open(path, encoding="utf-8") as query:
        return query.read()


def chunk_query(query: str | None) -> tuple[list[str]]:
    """Chunk multiquery to queryes."""

    if not query:
        return [], []

    pattern = r";(?=(?:[^']*'[^']*')*[^']*$)"
    parts = [
        part.strip(";").strip()
        for part in split(pattern, query)
        if part.strip(";").strip()
    ]

    if not parts:
        return [], []

    first_part: list[str] = []
    second_part: list[str] = []

    for i, part in enumerate(parts):
        first_part.append(part)

        if (i + 1 < len(parts) and parts[i + 1].lower().startswith(
                ('with ', 'select ')
            )
        ):
            second_part = parts[i + 1:]
            break
    else:
        second_part = []

    return first_part, second_part
