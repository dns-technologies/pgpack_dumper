import logging
from base_dumper import (
    DumpFormat,
    CompressionMethod,
    CompressionLevel,
    DumperMode,
)
from pgpack_dumper import PGPackDumper


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class TestSimplePGPackDumper:
    """Простые тесты для проверки базовой функциональности."""

    def test_dumper_creation(self, pg_connector):
        """Тестирует создание дампера."""

        dumper = PGPackDumper(
            connector=pg_connector,
            compression_method=CompressionMethod.ZSTD,
            compression_level=CompressionLevel.ZSTD_DEFAULT,
            dump_format=DumpFormat.BINARY,
            mode=DumperMode.PROD,
            logger=logger,
        )
        assert dumper is not None  # noqa: S101
        dumper.close()

    def test_connection(self, pg_connector):
        """Тестирует соединение с БД."""

        dumper = PGPackDumper(
            connector=pg_connector,
            mode=DumperMode.PROD,
            logger=logger,
        )

        try:
            assert dumper.connect is not None  # noqa: S101
            assert not dumper.connect.closed  # noqa: S101

            with dumper.cursor as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                assert result[0] == 1  # noqa: S101

        finally:
            dumper.close()

    def test_create_and_drop_table(self, pg_connector):
        """Тестирует создание и удаление таблицы."""

        dumper = PGPackDumper(
            connector=pg_connector,
            mode=DumperMode.PROD,
            logger=logger,
        )
        table_name = "test_temp_table"

        try:
            with dumper.cursor as cur:
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100)
                    )
                """)
                dumper.connect.commit()

            dumper.refresh()

            with dumper.cursor as cur:
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = '{table_name}'
                    )
                """)
                exists = cur.fetchone()[0]
                assert exists is True  # noqa: S101

        finally:
            try:
                dumper.refresh()
                with dumper.cursor as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                    dumper.connect.commit()
            except Exception as e:
                logging.warning(f"Failed to drop table: {e}")
            finally:
                dumper.close()
