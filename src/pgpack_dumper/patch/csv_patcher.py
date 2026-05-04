from io import StringIO
from json import loads

from pglast.parser import parse_sql_json

from .common import (
    Error,
    ResClass,
    TIMESTAMP,
)


def __column_fields(
    stmt: dict[str, dict[str, str | int]],
) -> list[dict[str, dict[str, str | int]]]:

    return stmt["ResTarget"]["val"]["ColumnRef"]["fields"]


def __target_list(tree: dict[str, dict]) -> list[dict[str, dict]]:
    return tree["SelectStmt"].get("targetList", [])


def __current_select(
    stmts: list[dict[str, str | int]],
) -> list[dict[str, str | int]]:
    if not stmts:
        return []

    last_stmt = stmts[-1]
    select_stmt: dict[str, dict] = last_stmt.get("stmt", {})

    if "SelectStmt" in select_stmt:
        return __target_list(select_stmt)

    if "WithClause" in select_stmt:
        cte_list: list[dict] = select_stmt.get("ctes", [])

        if cte_list:
            cte: dict[str, dict] = cte_list[-1].get("CommonTableExpr", {})
            cte_select: dict[str, dict] = cte.get("ctequery", {})

            if "SelectStmt" in cte_select:
                return __target_list(cte_select)

    return []


def __is_star(stmt: dict[str, dict[str, str | int]]) -> bool:
    if stmt["ResTarget"].get("name"):
        return False

    if "ColumnRef" not in stmt["ResTarget"]["val"]:
        return False

    if "A_Star" in __column_fields(stmt)[-1]:
        return True

    return False


def __column_class(
    stmt: dict[str, dict[str, dict[str, str | int]]],
) -> ResClass:
    return ResClass(tuple(stmt["ResTarget"]["val"].keys())[0])


def __column_name(stmt: dict[str, dict[str, str | int]]) -> str:
    if name := stmt["ResTarget"].get("name"):
        return name

    val = stmt["ResTarget"]["val"]

    if "TypeCast" in val:
        return __column_first_word(val)

    string = __column_fields(stmt)[-1].get("String")

    if not string:
        return ""

    return string["sval"]


def __column_first_word(val: dict[str, dict[str, str]]) -> str:

    if type_cast := val.get("TypeCast"):
        return __column_first_word(type_cast["arg"])

    if const := val.get("A_Const"):
        return const["sval"]["sval"]

    if column := val.get("ColumnRef"):
        return column["fields"][-1]["String"]["sval"]

    if expr := val.get("A_Expr"):
        return __column_first_word(expr["lexpr"])

    if func_call := val.get("FuncCall"):
        return func_call["funcname"][-1]["String"]["sval"]

    if sql_value := val.get("SQLValueFunction"):
        return sql_value["op"].replace("SVFOP_", "")


def __column_start_position(
    stmt: dict[str, dict[str, str | int]],
    res_class: ResClass,
    query: str,
) -> int:
    location = stmt["ResTarget"]["location"]

    if res_class in (ResClass.COLUMN, ResClass.CONST):
        return location

    search_word = __column_first_word(stmt["ResTarget"]["val"]).upper()
    search_position = len(search_word)
    current_location = -1

    while current_location == -1:
        current_location = query.upper().find(
            search_word,
            location - search_position,
        )
        search_position += len(search_word)

    if current_location > location:
        current_location = location

    if query[current_location - 1] in "('\"":
        return current_location - 1

    return current_location


def __column_end_position(
    query: str,
    start: int,
    res_class: ResClass,
    name: str,
) -> int:
    depth = 0
    in_quotes = False
    quote_char = None

    for end_pos in range(start, len(query)):
        ch = query[end_pos]

        if ch in "'\"" and not in_quotes:
            in_quotes = True
            quote_char = ch
        elif ch == quote_char and in_quotes:
            in_quotes = False
            quote_char = None

        if not in_quotes:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1

        if ch in "\t\r\n ," and depth == 0 and not in_quotes:
            if res_class is not ResClass.EXPR:
                return end_pos

            finder = query[end_pos:]
            end_pos += min(finder.index(name), finder.casefold().index(" as "))

            while query[end_pos - 1] in "\t\r\n ,":
                end_pos -= 1

            return end_pos

    return len(query)


def __nested_columns_list(
    select: list[dict[str, str | int]],
    column_list: list[str],
) -> list:
    nested = []
    col_idx = 0

    for stmt in select:
        if __is_star(stmt):
            star_columns = []
            next_name = None

            for s in select[select.index(stmt) + 1:]:
                if not __is_star(s):
                    next_name = __column_name(s)
                    break

            while col_idx < len(column_list):
                col = column_list[col_idx]
                if col == next_name:
                    break
                star_columns.append(col)
                col_idx += 1

            nested.append(star_columns)
        else:
            nested.append(column_list[col_idx])
            col_idx += 1

    return nested


def patch_csv_timestamp(
    query: str | None,
    table: str | None,
    columns: dict[str, str],
) -> tuple[str, ...]:
    if not query and not table:
        raise Error.CSVPatcherValueError("Query or table not define.")

    if not columns:
        raise Error.CSVPatcherValueError("Columns not define.")

    if not isinstance(columns, dict):
        raise Error.CSVPatcherTypeError(
            "Columns must be dict with {column_name: column_value} structure.",
        )

    patch_items: list[str] = []

    for column, dtype in columns.items():
        if dtype.startswith("timestamp"):
            patch_items.append(column)

    if not patch_items:
        return query, table

    if not query:
        select_parts = []

        for column in columns:
            if column in patch_items:
                select_parts.append(f"\"{column}\"{TIMESTAMP}")
            else:
                select_parts.append(f"\"{column}\"")

        column_list = ", ".join(select_parts)
        return f"SELECT {column_list} FROM {table}", table

    patched_query = StringIO()
    stmts = loads(parse_sql_json(query))["stmts"]
    select = __current_select(stmts)
    column_list = list(columns)

    if len(select) != len(column_list):
        column_list = __nested_columns_list(select, column_list)

    if len(select) != len(column_list):
        raise Error.CSVPatcherValueError("Columns not match.")

    last_position = 0

    for num, (column, stmt) in enumerate(zip(column_list, select), 1):
        res_class = __column_class(stmt)
        name = __column_name(stmt)
        start = __column_start_position(stmt, res_class, query)
        end = __column_end_position(query, start, res_class, name)

        if __is_star(stmt):
            patched_query.write(query[last_position:start])
            col_parts = []

            for col in column:
                if col in patch_items:
                    col_parts.append(f"\"{col}\"{TIMESTAMP}")
                else:
                    col_parts.append(f"\"{col}\"")

            patched_query.write(", ".join(col_parts))

            if num < len(column_list) and col_parts and query[end] != ",":
                    patched_query.write(",")

        elif column in patch_items:
            patched_query.write(query[last_position: start])
            column_expr = query[start:end]

            if TIMESTAMP in column_expr:
                patched_query.write(column_expr)
            elif (
                res_class in (ResClass.CAST, ResClass.EXPR)
                and column_expr[0] != "("
            ):
                patched_query.write(f"({column_expr}){TIMESTAMP}")
            else:
                patched_query.write(f"{column_expr}{TIMESTAMP}")
        else:
            patched_query.write(query[last_position: end])

        last_position = end

    patched_query.write(query[last_position:])
    return patched_query.getvalue(), table
