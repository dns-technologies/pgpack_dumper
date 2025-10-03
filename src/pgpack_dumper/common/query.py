from pathlib import Path
from random import randbytes
from re import match


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

    first_part: list[str] = [
        part.strip()
        for part in query.split(";")
    ]
    second_part: list[str] = []

    for _ in first_part:
        second_part.append(first_part.pop())
        if any(
            word == second_part[-1][:len(word)].lower()
            for word in ("with", "select")
        ):
            second_part = list(reversed(second_part))
            break

    return first_part, second_part


