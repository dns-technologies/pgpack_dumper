from typing import Any

from base_dumper import DebugInfo
from sqlparse import parse


MEMORY_FACTOR = 1024


def get_query_kind(query: str) -> str:
    """Get kind of query."""

    return parse(query)[0].get_type().capitalize()


def get_info(
    dbname: str,
    host: str,
    kind: str,
    explain: list[dict[str, Any]],
) -> DebugInfo:
    """Get DebugInfo from explain."""

    main_data = explain[0]
    plan: dict[str, Any] = main_data["Plan"]
    duration = round(main_data["Execution Time"] / 1000)
    rows = plan["Actual Rows"]
    storage = plan["Plan Width"] * rows
    memory = 0

    if statistics := main_data.get("Slice statistics"):
        slice_stat: dict[str, Any]

        for slice_stat in statistics:
            mem_info = slice_stat.get("Executor Memory")

            if isinstance(mem_info, dict):
                slice_memory = mem_info.get("Maximum Memory Used", 0)

                if dbname == "postgres":
                    if max_mem := slice_stat.get("Work Maximum Memory"):
                        slice_memory += max_mem
            elif isinstance(mem_info, int):
                slice_memory = mem_info

            memory = max(memory, slice_memory)

    if dbname == "greenplum":
        memory *= MEMORY_FACTOR

    if usage := plan.get("Peak Memory Usage"):
        memory = memory or usage * MEMORY_FACTOR

    return DebugInfo(
        host,
        kind,
        duration,
        memory,
        storage,
        rows,
    )
