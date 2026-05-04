"""Tests for postgres_csvpatcher."""

import pytest
from pgpack_dumper.patch import (
    Error,
    patch_csv_timestamp,
)


COLUMNS = {
    "id": "int4",
    "name": "varchar",
    "created_at": "timestamp",
    "updated_at": "timestamptz",
    "_date_load": "timestamp",
}

TABLE = "users"


class TestBasicColumns:
    """Tests for simple column patching."""

    def test_simple_select_with_timestamp(self):
        query = "SELECT id, created_at FROM users"
        expected = "SELECT id, created_at::timestamp(0) FROM users"
        columns = {"id": "int4", "created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, TABLE, columns)
        assert result == expected  # noqa: S101

    def test_select_with_alias(self):
        query = "SELECT id, created_at as ct FROM users"
        expected = "SELECT id, created_at::timestamp(0) as ct FROM users"
        columns = {"id": "int4", "created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, TABLE, columns)
        assert result == expected  # noqa: S101

    def test_multiple_timestamp_columns(self):
        query = "SELECT id, created_at, updated_at FROM users"
        expected = (
            "SELECT id, created_at::timestamp(0), "
            "updated_at::timestamp(0) FROM users"
        )
        columns = {
            "id": "int4",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        }
        result, _ = patch_csv_timestamp(query, TABLE, columns)
        assert result == expected  # noqa: S101

    def test_quoted_column_names(self):
        query = 'SELECT id, "created_at" FROM users'
        expected = 'SELECT id, "created_at"::timestamp(0) FROM users'
        columns = {"id": "int4", "created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, TABLE, columns)
        assert result == expected  # noqa: S101

    def test_already_has_timestamp_cast(self):
        query = "SELECT id, created_at::timestamp(0) FROM users"
        expected = "SELECT id, created_at::timestamp(0) FROM users"
        columns = {"id": "int4", "created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, TABLE, columns)
        assert result == expected  # noqa: S101

    def test_no_timestamp_columns(self):
        columns = {"id": "int4", "name": "varchar"}
        query = "SELECT id, name FROM users"
        result, _ = patch_csv_timestamp(query, TABLE, columns)
        assert result == query  # noqa: S101


class TestFunctionsAndExpressions:
    """Tests for functions and expressions."""

    def test_function_now(self):
        query = "SELECT id, now() as _date_load FROM orders"
        expected = "SELECT id, now()::timestamp(0) as _date_load FROM orders"
        columns = {"id": "int4", "_date_load": "timestamp"}
        result, _ = patch_csv_timestamp(query, "orders", columns)
        assert result == expected  # noqa: S101

    def test_function_date_trunc(self):
        query = (
            "SELECT date_trunc('month', created_at) as _date_load FROM orders"
        )
        expected = (
            "SELECT date_trunc('month', created_at)::timestamp(0) "
            "as _date_load FROM orders"
        )
        columns = {"_date_load": "timestamp"}
        result, _ = patch_csv_timestamp(query, "orders", columns)
        assert result == expected  # noqa: S101

    def test_expression_with_interval(self):
        query = (
            "SELECT created_at + INTERVAL '1 day' as _date_load FROM orders"
        )
        expected = (
            "SELECT (created_at + INTERVAL '1 day')::timestamp(0) "
            "as _date_load FROM orders"
        )
        columns = {"_date_load": "timestamp"}
        result, _ = patch_csv_timestamp(query, "orders", columns)
        assert result == expected  # noqa: S101

    def test_current_timestamp(self):
        query = "SELECT CURRENT_TIMESTAMP as _today FROM events"
        expected = (
            "SELECT CURRENT_TIMESTAMP::timestamp(0) as _today FROM events"
        )
        columns = {"_today": "timestamp"}
        result, _ = patch_csv_timestamp(query, "events", columns)
        assert result == expected  # noqa: S101

    def test_nested_function(self):
        query = "SELECT EXTRACT(HOUR FROM created_at) as hour FROM events"
        expected = (
            "SELECT EXTRACT(HOUR FROM created_at)::timestamp(0) "
            "as hour FROM events"
        )
        columns = {"hour": "timestamp"}
        result, _ = patch_csv_timestamp(query, "events", columns)
        assert result == expected  # noqa: S101


class TestWildcardExpansion:
    """Tests for SELECT * expansion."""

    def test_select_star(self):
        query = "SELECT * FROM users"
        expected = (
            'SELECT "id", "name", "created_at"::timestamp(0), '
            '"updated_at"::timestamp(0), "_date_load"::timestamp(0) FROM users'
        )
        result, _ = patch_csv_timestamp(query, TABLE, COLUMNS)
        assert result == expected  # noqa: S101

    def test_table_star(self):
        query = "SELECT u.* FROM users u"
        expected = (
            'SELECT "id", "name", "created_at"::timestamp(0), '
            '"updated_at"::timestamp(0), "_date_load"::timestamp(0) '
            "FROM users u"
        )
        result, _ = patch_csv_timestamp(query, TABLE, COLUMNS)
        assert result == expected  # noqa: S101


class TestJoinQueries:
    """Tests for JOIN queries with table prefixes."""

    def test_join_with_prefixes(self):
        query = """
        SELECT
            e.id,
            e.created_at,
            u.updated_at as user_updated
        FROM events e
        JOIN users u ON e.id = u.id
        """
        expected = """
        SELECT
            e.id,
            e.created_at::timestamp(0),
            u.updated_at::timestamp(0) as user_updated
        FROM events e
        JOIN users u ON e.id = u.id
        """
        columns = {
            "id": "int4",
            "created_at": "timestamp",
            "updated_at": "timestamptz",
        }
        result, _ = patch_csv_timestamp(query, "events", columns)
        assert result == expected  # noqa: S101


class TestMultiquery:
    """Tests for multiquery (multiple statements separated by ;)."""

    def test_last_select_is_patched(self):
        query = "SELECT 1; SELECT id, created_at FROM users"
        expected = "SELECT 1; SELECT id, created_at::timestamp(0) FROM users"
        columns = {"id": "int4", "created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, TABLE, columns)
        assert result == expected  # noqa: S101


class TestQueryNone:
    """Tests for query generation from table name."""

    def test_query_none_builds_from_table(self):
        result, table = patch_csv_timestamp(None, TABLE, COLUMNS)
        assert table == TABLE  # noqa: S101
        assert "SELECT" in result  # noqa: S101
        assert "FROM" in result  # noqa: S101
        assert "::timestamp(0)" in result  # noqa: S101
        assert '"_date_load"::timestamp(0)' in result  # noqa: S101

    def test_query_none_no_timestamp_columns(self):
        columns = {"id": "int4", "name": "varchar"}
        result, table = patch_csv_timestamp(None, TABLE, columns)
        assert result is None  # noqa: S101


class TestEdgeCases:
    """Tests for edge cases."""

    def test_mixed_case_keywords(self):
        query = "select id, created_at from events"
        expected = "select id, created_at::timestamp(0) from events"
        columns = {"id": "int4", "created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, "events", columns)
        assert result == expected  # noqa: S101

    def test_multiple_spaces_and_newlines(self):
        query = "SELECT   id,    \n   created_at    \nFROM   events"
        expected = (
            "SELECT   id,    \n   created_at::timestamp(0)    \nFROM   events"
        )
        columns = {"id": "int4", "created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, "events", columns)
        assert result == expected  # noqa: S101

    def test_column_name_substring(self):
        query = "SELECT ts, tsz FROM events"
        expected = "SELECT ts::timestamp(0), tsz::timestamp(0) FROM events"
        columns = {"ts": "timestamp", "tsz": "timestamptz"}
        result, _ = patch_csv_timestamp(query, "events", columns)
        assert result == expected  # noqa: S101

    def test_column_with_table_alias(self):
        query = "SELECT u.created_at as user_created FROM users u"
        expected = (
            "SELECT u.created_at::timestamp(0) as user_created FROM users u"
        )
        columns = {"created_at": "timestamp"}
        result, _ = patch_csv_timestamp(query, "users", columns)
        assert result == expected  # noqa: S101


class TestErrors:
    """Tests for error handling."""

    def test_empty_query_and_table_raises(self):
        with pytest.raises(Error.CSVPatcherValueError):
            patch_csv_timestamp(None, None, COLUMNS)

    def test_empty_columns_raises(self):
        with pytest.raises(Error.CSVPatcherValueError):
            patch_csv_timestamp("SELECT 1", TABLE, {})

    def test_columns_not_dict_raises(self):
        with pytest.raises(Error.CSVPatcherTypeError):
            patch_csv_timestamp("SELECT 1", TABLE, ["id", "name"])


class TestTable:
    """Tests for simple column patching."""

    def test_table_star_with_alias_column(self):
        """Test that table.* expansion doesn't duplicate commas."""
        query = 'select orders.*, "_date_load" from orders'
        expected = (
            'select "id", "created_at", "_date_load"::timestamp(0) from orders'
        )
        columns = {
            "id": "int4",
            "created_at": "date",
            "_date_load": "timestamp",
        }
        result, _ = patch_csv_timestamp(query, None, columns)
        assert result == expected  # noqa: S101

    def test_query_none_builds_from_table(self):
        """Test query generation when query is None."""
        columns = {
            "id": "int4",
            "created_at": "timestamp",
            "_date_load": "timestamp",
        }
        result, table = patch_csv_timestamp(None, TABLE, columns)
        expected = (
            'SELECT "id", "created_at"::timestamp(0), '
            '"_date_load"::timestamp(0) FROM users'
        )
        assert result == expected  # noqa: S101
        assert table == TABLE  # noqa: S101
