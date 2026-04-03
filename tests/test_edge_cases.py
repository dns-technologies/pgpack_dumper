import pytest
from psycopg import errors as psycopg_errors
from pgpack import PGPackReader


class TestEdgeCases:
    """Тесты граничных случаев и ошибок."""

    def test_empty_table_dump(self, dumper, test_table, tmp_path):
        """Тестирует создание дампа пустой таблицы."""

        dump_file = tmp_path / "empty_dump.pgpack"

        with open(dump_file, "wb") as f:
            dumper.read_dump(f, table_name=test_table)

        assert dump_file.exists()  # noqa: S101
        assert dump_file.stat().st_size > 0  # noqa: S101

        with open(dump_file, "rb") as f:

            reader = PGPackReader(f)
            assert reader.columns == ["name", "age"]  # noqa: S101

        with open(dump_file, "rb") as f:
            reader = PGPackReader(f)
            df = reader.to_pandas()
            assert df.empty, "DataFrame should be empty"  # noqa: S101

        with open(dump_file, "rb") as f:
            reader = PGPackReader(f)
            pl_df = reader.to_polars()
            assert pl_df.is_empty(), "Polars DataFrame should be empty"  # noqa: S101

    def test_nonexistent_table_error(self, dumper):
        """Тестирует ошибку при работе с несуществующей таблицей."""

        with pytest.raises(psycopg_errors.UndefinedTable):
            dumper.metadata(table_name="nonexistent_table_12345")

    def test_invalid_query_error(self, dumper):
        """Тестирует ошибку при невалидном запросе."""

        with pytest.raises(
            (psycopg_errors.UndefinedTable, psycopg_errors.SyntaxError)
        ):
            dumper.metadata(query="SELECT FROM invalid_syntax")

    def test_large_data_transfer(self, dumper, test_table, tmp_path):
        """Тестирует передачу большого объема данных."""

        large_data = [(f"User_{i}", i % 100) for i in range(10000)]
        dumper.from_rows(large_data, test_table)

        with dumper.cursor as cur:
            cur.execute(f"SELECT COUNT(*) FROM {test_table}")
            count = cur.fetchone()[0]
            assert count == 10000  # noqa: S101

        dumper.refresh()
        dump_file = tmp_path / "large_dump.pgpack"

        with open(dump_file, "wb") as f:
            dumper.read_dump(f, table_name=test_table)

        assert dump_file.stat().st_size > 0  # noqa: S101
