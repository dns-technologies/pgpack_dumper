from pathlib import Path
from re import match

PATTERN = r"\(select \* from (.*)\)|(.*)"


def search_object(table: str, query: str = "") -> str:
    """Return current string for object."""

    if query:
        return "query"

    return match(PATTERN, table).group(1) or table


def query_path() -> str:
    """Path for queryes."""

    return f"{Path(__file__).parent.absolute()}/queryes/{{}}.sql"


def query_template(query_name: str) -> str:
    """Get query template for his name."""

    path = query_path().format(query_name)

    with open(path, encoding="utf-8") as query:
        return query.read()
