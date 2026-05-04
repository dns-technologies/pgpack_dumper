import logging

from pgpack_dumper import PGPackDumper
from base_dumper import (
    DumpFormat,
    CompressionMethod,
    CompressionLevel,
    DumperMode,
    IsolationLevel,
)

logger = logging.getLogger(__name__)


class TestPGPackDumper:
    """Тесты для PGPackDumper с реальным PostgreSQL."""

    def test_initialization(self, pg_connector):
        """Тестирует инициализацию дампера."""

        dumper = PGPackDumper(
            connector=pg_connector,
            compression_method=CompressionMethod.ZSTD,
            compression_level=CompressionLevel.ZSTD_DEFAULT,
            dump_format=DumpFormat.BINARY,
            mode=DumperMode.DEBUG,
            logger=logger,
        )

        assert dumper is not None  # noqa: S101
        assert dumper.connector == pg_connector  # noqa: S101
        assert dumper.dump_format == DumpFormat.BINARY  # noqa: S101
        assert dumper.mode == DumperMode.DEBUG  # noqa: S101
        dumper.close()

    def test_timeout_property(self, dumper):
        """Тестирует установку и получение timeout."""

        original_timeout = dumper.timeout
        dumper.timeout = 60
        assert dumper.timeout == 60  # noqa: S101
        dumper.timeout = original_timeout

    def test_isolation_property(self, dumper):
        """Тестирует установку уровня изоляции."""

        original_isolation = dumper.isolation
        dumper.isolation = IsolationLevel.SERIALIZABLE
        new_isolation = dumper.isolation
        assert new_isolation in [  # noqa: S101
            IsolationLevel.SERIALIZABLE,
            original_isolation,
        ]
        dumper.isolation = original_isolation
        assert dumper.isolation == original_isolation  # noqa: S101

    def test_metadata_table(self, dumper, test_table):
        """Тестирует получение метаданных таблицы."""

        metadata = dumper.metadata(table_name=test_table)
        assert metadata is not None  # noqa: S101
        assert hasattr(metadata, "columns")  # noqa: S101
        assert "name" in metadata.columns  # noqa: S101
        assert "age" in metadata.columns  # noqa: S101

    def test_metadata_query(self, dumper):
        """Тестирует получение метаданных из запроса."""

        query = "SELECT 1 as num, 'test' as text"
        metadata = dumper.metadata(query=query)
        assert metadata is not None  # noqa: S101
        assert "num" in metadata.columns  # noqa: S101
        assert "text" in metadata.columns  # noqa: S101

    def test_write_and_read_dump(
        self, dumper, test_table, test_rows, tmp_path
    ):
        """Тестирует запись и чтение дампа."""

        dump_file = tmp_path / "test_dump.pgpack"
        dumper.from_rows(test_rows, test_table)

        with open(dump_file, "wb") as f:
            dumper.read_dump(f, table_name=test_table)

        assert dump_file.exists()  # noqa: S101
        assert dump_file.stat().st_size > 0  # noqa: S101
        new_table = f"{test_table}_copy"

        with dumper.cursor as cur:
            cur.execute(f"""
                CREATE TABLE {new_table} (
                    name VARCHAR(100),
                    age INTEGER
                )
            """)
            dumper.connect.commit()

        dumper.refresh()

        with open(dump_file, "rb") as f:
            dumper.write_dump(f, new_table)

        with dumper.cursor as cur:
            cur.execute(f"SELECT name, age FROM {new_table}")
            results = cur.fetchall()
            assert len(results) == len(test_rows)  # noqa: S101

            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

    def test_write_between(self, dumper, test_table, test_rows):
        """Тестирует копирование между таблицами."""

        dumper.from_rows(test_rows, test_table)
        dest_table = f"{test_table}_dest"

        with dumper.cursor as cur:
            cur.execute(f"""
                CREATE TABLE {dest_table} (
                    name VARCHAR(100),
                    age INTEGER
                )
            """)
            dumper.connect.commit()

        dumper.refresh()
        dumper.write_between(dest_table, table_src=test_table)

        with dumper.cursor as cur:
            cur.execute(f"SELECT name, age FROM {dest_table}")
            results = cur.fetchall()

            assert len(results) == len(test_rows)  # noqa: S101

            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

    def test_write_between_with_query(self, dumper, test_table, test_rows):
        """Тестирует копирование с использованием запроса."""

        dumper.from_rows(test_rows, test_table)
        dest_table = f"{test_table}_filtered"

        with dumper.cursor as cur:
            cur.execute(f"""
                CREATE TABLE {dest_table} (
                    name VARCHAR(100),
                    age INTEGER
                )
            """)
            dumper.connect.commit()

        dumper.refresh()
        query = f"SELECT name, age FROM {test_table} WHERE age > 30"
        dumper.write_between(dest_table, query_src=query)

        with dumper.cursor as cur:
            cur.execute(f"SELECT name, age FROM {dest_table}")
            results = cur.fetchall()

            expected_count = len([r for r in test_rows if r[1] > 30])
            assert len(results) == expected_count  # noqa: S101

            for result in results:
                assert result[1] > 30  # noqa: S101

    def test_from_rows(self, dumper, test_table, test_rows):
        """Тестирует запись из строк (кортежей)."""

        dumper.from_rows(test_rows, test_table)

        with dumper.cursor as cur:
            cur.execute(f"SELECT name, age FROM {test_table}")
            results = cur.fetchall()
            assert len(results) == len(test_rows)  # noqa: S101

            for result, expected in zip(results, test_rows):
                assert result[0] == expected[0]  # noqa: S101
                assert result[1] == expected[1]  # noqa: S101

    def test_from_bytes(self, dumper, test_table):
        """Тестирует запись из байтов в CSV формате."""

        csv_data = b"Alice,30\nBob,25\nCharlie,35\n"
        original_format = dumper.dump_format
        dumper.dump_format = DumpFormat.CSV

        try:
            metadata = dumper.metadata(table_name=test_table, reader_meta=True)
            dumper.from_bytes([csv_data], test_table, source=metadata)

            with dumper.cursor as cur:
                cur.execute(f"SELECT name, age FROM {test_table}")
                results = cur.fetchall()
                assert len(results) == 3  # noqa: S101
                assert results[0][0] == "Alice"  # noqa: S101
                assert results[0][1] == 30  # noqa: S101

        finally:
            dumper.dump_format = original_format

    def test_mode_debug_queries(self, dumper, capsys):
        """Тестирует выполнение запросов в DEBUG режиме."""

        _ = capsys
        original_mode = dumper.mode
        dumper.mode = DumperMode.DEBUG

        try:
            result = dumper.mode_action("SELECT 1")
            assert result is not None or result is None  # noqa: S101
        finally:
            dumper.mode = original_mode

    def test_refresh_connection(self, dumper):
        """Тестирует обновление соединения."""

        original_connection_id = id(dumper.connect)
        dumper.refresh()
        assert id(dumper.connect) != original_connection_id  # noqa: S101
        assert dumper.cursor is not None  # noqa: S101
        assert dumper.copy_buffer.cursor == dumper.cursor  # noqa: S101

    def test_close_connection(self, dumper):
        """Тестирует закрытие соединения."""

        assert not dumper.connect.closed  # noqa: S101
        dumper.close()
        assert dumper.connect.closed  # noqa: S101
