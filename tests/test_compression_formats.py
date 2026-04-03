import pytest
import logging
from base_dumper import (
    DumpFormat,
    CompressionMethod,
    CompressionLevel,
    DumperMode,
)
from pgpack_dumper import PGPackDumper

test_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class TestCompressionFormats:
    """Тесты различных форматов сжатия."""

    @pytest.mark.parametrize(
        "compression_method",
        [
            CompressionMethod.ZSTD,
            CompressionMethod.GZIP,
            CompressionMethod.LZ4,
            CompressionMethod.SNAPPY,
        ],
    )
    def test_compression_methods(
        self, pg_connector, compression_method, test_table, test_data, tmp_path
    ):
        """Тестирует все методы сжатия."""

        dumper = PGPackDumper(
            connector=pg_connector,
            compression_method=compression_method,
            compression_level=CompressionLevel.ZSTD_DEFAULT,
            dump_format=DumpFormat.BINARY,
            mode=DumperMode.PROD,
            logger=test_logger,
        )

        try:
            rows = [(item["name"], item["age"]) for item in test_data]
            dumper.from_rows(rows, test_table)
            dump_file = tmp_path / f"dump_{compression_method.name}.pgpack"

            with open(dump_file, "wb") as f:
                dumper.read_dump(f, table_name=test_table)

            assert dump_file.exists()  # noqa: S101
            assert dump_file.stat().st_size > 0  # noqa: S101

            with dumper.cursor as cur:
                cur.execute(f"SELECT COUNT(*) FROM {test_table}")
                count = cur.fetchone()[0]
                assert count == len(test_data)  # noqa: S101

        finally:
            dumper.close()

    @pytest.mark.parametrize(
        "dump_format", [DumpFormat.BINARY, DumpFormat.CSV]
    )
    def test_dump_formats(
        self, pg_connector, dump_format, test_table, test_data, tmp_path
    ):
        """Тестирует форматы дампа BINARY и CSV."""

        dumper = PGPackDumper(
            connector=pg_connector,
            dump_format=dump_format,
            mode=DumperMode.PROD,
            logger=test_logger,
        )

        try:
            rows = [(item["name"], item["age"]) for item in test_data]
            dumper.from_rows(rows, test_table)
            dump_file = tmp_path / f"dump_{dump_format.name}.pgpack"

            with open(dump_file, "wb") as f:
                dumper.read_dump(f, table_name=test_table)

            assert dump_file.exists()  # noqa: S101
            assert dump_file.stat().st_size > 0  # noqa: S101
            new_table = f"{test_table}_{dump_format.name}"

            with dumper.cursor as cur:
                cur.execute(f"""
                    CREATE TABLE {new_table} (
                        name VARCHAR(100),
                        age INTEGER
                    )
                """)
                dumper.connect.commit()

            dumper.refresh()
            writer_dumper = PGPackDumper(
                connector=pg_connector,
                dump_format=dump_format,
                mode=DumperMode.PROD,
                logger=test_logger,
            )

            try:
                with open(dump_file, "rb") as f:
                    writer_dumper.write_dump(f, new_table)

                with writer_dumper.cursor as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {new_table}")
                    count = cur.fetchone()[0]
                    assert count == len(test_data)  # noqa: S101
            finally:
                writer_dumper.close()

        finally:
            dumper.close()
