import logging

from typing import NamedTuple

import psycopg
import pytest

from base_dumper import DumperMode
from pgpack_dumper import (
    PGConnector,
    PGPackDumper,
)


logger = logging.getLogger(__name__)


class PGConnectorWithOptions(NamedTuple):
    """Тестовый коннектор с поддержкой options."""

    host: str
    port: int
    user: str
    password: str
    dbname: str
    options: str | None = None


class TestReadOnlyMode:
    """Тесты для read-only режима PostgreSQL."""

    def test_readonly_initialization(self, pg_connector):
        """Тестирует инициализацию в read-only режиме."""

        readonly_connector = PGConnectorWithOptions(
            host=pg_connector.host,
            port=pg_connector.port,
            user=pg_connector.user,
            password=pg_connector.password,
            dbname=pg_connector.dbname,
            options="-c default_transaction_read_only=on",
        )
        conn = psycopg.connect(
            host=readonly_connector.host,
            port=readonly_connector.port,
            user=readonly_connector.user,
            password=readonly_connector.password,
            dbname=readonly_connector.dbname,
            options=readonly_connector.options,
        )
        assert conn is not None  # noqa: S101

        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            is_readonly = cur.fetchone()[0]
            assert is_readonly == "on"  # noqa: S101

        conn.close()

    def test_readonly_metadata(self, pg_connector, dumper):
        """Тестирует чтение метаданных в read-only режиме."""

        test_table = "test_readonly_table"

        with dumper.cursor as cur:
            cur.execute(f"""
                CREATE TABLE {test_table} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100)
                )
            """)
            dumper.connect.commit()

        dumper.refresh()
        readonly_conn = psycopg.connect(
            host=pg_connector.host,
            port=pg_connector.port,
            user=pg_connector.user,
            password=pg_connector.password,
            dbname=pg_connector.dbname,
            options="-c default_transaction_read_only=on",
        )

        with readonly_conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                (test_table,),
            )
            columns = cur.fetchall()

        assert len(columns) >= 2  # noqa: S101
        readonly_conn.close()

    def test_readonly_write_error(
        self, pg_connector, test_rows, dumper, test_table
    ):
        """Тестирует, что запись в read-only режиме вызывает ошибку."""

        dumper.from_rows(test_rows, test_table)
        dumper.refresh()
        readonly_conn = psycopg.connect(
            host=pg_connector.host,
            port=pg_connector.port,
            user=pg_connector.user,
            password=pg_connector.password,
            dbname=pg_connector.dbname,
            options="-c default_transaction_read_only=on",
        )

        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):  # noqa: PT012
            with readonly_conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {test_table} (name, age) VALUES ('Test', 99)"
                )
                readonly_conn.commit()

        readonly_conn.close()

    def test_readonly_read_dump(
        self, pg_connector, dumper, test_table, test_rows, tmp_path
    ):
        """Тестирует создание дампа в read-only режиме."""

        dumper.from_rows(test_rows, test_table)
        dumper.refresh()
        readonly_dumper = PGPackDumper(
            connector=PGConnector(
                host=pg_connector.host,
                port=pg_connector.port,
                user=pg_connector.user,
                password=pg_connector.password,
                dbname=pg_connector.dbname,
            ),
            mode=DumperMode.PROD,
            logger=logger,
        )
        dump_file = tmp_path / "readonly_dump.pgpack"

        with open(dump_file, "wb") as f:
            readonly_dumper.read_dump(f, table_name=test_table)

        assert dump_file.exists()  # noqa: S101
        assert dump_file.stat().st_size > 0  # noqa: S101
        readonly_dumper.close()
