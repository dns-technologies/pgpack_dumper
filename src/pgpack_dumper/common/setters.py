from base_dumper import IsolationLevel


TIME_FACTOR = {
    "ms": 0.001,
    "s": 1,
    "min": 60,
    "h": 3600,
    "d": 86_400,
}

def statement_seconds(statement: str) -> int | float:
    """Get seconds from statement_timeout."""

    value = ""
    factor = ""

    for char in statement:
        if char.isdigit():
            value += char
        if char.isalpha():
            factor += char

    return int(value) * TIME_FACTOR[factor]


def isolation_level(level: str) -> IsolationLevel:
    """Get transaction isolation level."""

    return IsolationLevel(level.upper())
