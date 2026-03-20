from typing import Any

from base_dumper import DebugInfo
from sqlparse import parse


def get_query_kind(query: str) -> str:
    """Get kind of query."""

    return parse(query)[0].get_type().capitalize()


def get_info(
    host: str,
    kind: str,
    explain: list[dict[str, Any]],
) -> DebugInfo:
    """Get DebugInfo from explain."""

    main_data = explain[0]
    plan: dict[str, Any] = main_data["Plan"]
    duration = round(main_data["Execution Time"] / 1000, 3)
    rows = plan["Actual Rows"]
    storage = plan["Plan Width"] * rows

    def _get_max_memory(node: dict[str, Any] | None) -> int:
        """Recursively find maximum Peak Memory Usage in plan tree."""

        if not node:
            return 0

        max_memory = node.get("Peak Memory Usage", 0) * 1024

        if plans := node.get("Plans"):
            for subplan in plans:
                max_memory = max(max_memory, _get_max_memory(subplan))

        return max_memory

    memory = _get_max_memory(plan)

    return DebugInfo(
        host,
        kind,
        duration,
        memory,
        storage,
        rows,
    )
