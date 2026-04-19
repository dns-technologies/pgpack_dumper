import time
import random
import string
import logging

from collections.abc import Generator
from io import BytesIO
from typing import (
    Iterator,
    Iterable,
    Any,
)

import docker
import pandas as pd
import polars as pl
import psycopg
import pytest

from base_dumper import (
    BaseDumper,
    DBMetadata,
    DumperMode,
    DumpFormat,
    CompressionMethod,
    CompressionLevel,
)
from pgpack_dumper import (
    PGPackDumper,
    PGConnector,
)


POSTGRES_IMAGE = "postgres:15"
test_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


def random_string(length: int = 8) -> str:

    return "".join(random.choices(string.ascii_lowercase, k=length))  # noqa: S311


def wait_for_postgres(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    timeout: int = 30,
):
    """Ожидает готовности PostgreSQL."""

    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            conn = psycopg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=database,
                connect_timeout=2,
            )
            conn.close()
            return True
        except Exception as e:
            test_logger.debug(f"Waiting for PostgreSQL: {e}")
            time.sleep(1)
    raise TimeoutError(f"PostgreSQL not ready after {timeout} seconds")


@pytest.fixture(scope="session")
def docker_client():

    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture(scope="session")
def postgres_container(docker_client):
    """Создает контейнер PostgreSQL с правами на запись."""

    container_name = f"pgpack_test_{random_string()}"
    container = docker_client.containers.run(
        POSTGRES_IMAGE,
        name=container_name,
        environment={
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_DB": "testdb",
        },
        ports={"5432/tcp": None},
        detach=True,
        remove=False,
    )
    time.sleep(3)
    container.reload()
    host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0][
        "HostPort"
    ]
    wait_for_postgres(
        "localhost", int(host_port), "testuser", "testpass", "testdb"
    )
    yield {
        "container": container,
        "host": "localhost",
        "port": int(host_port),
        "user": "testuser",
        "password": "testpass",
        "database": "testdb",
    }
    try:
        container.stop(timeout=10)
        container.remove()
    except Exception as e:
        test_logger.warning(f"Failed to remove container: {e}")


@pytest.fixture(scope="session")
def readonly_postgres_container(docker_client):
    """Создает контейнер PostgreSQL в read-only режиме."""

    container_name = f"pgpack_readonly_test_{random_string()}"
    container = docker_client.containers.run(
        POSTGRES_IMAGE,
        name=container_name,
        environment={
            "POSTGRES_USER": "readonly_user",
            "POSTGRES_PASSWORD": "readonly_pass",
            "POSTGRES_DB": "readonly_db",
        },
        ports={"5432/tcp": None},
        detach=True,
        remove=False,
    )
    time.sleep(3)
    container.reload()
    host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0][
        "HostPort"
    ]
    wait_for_postgres(
        "localhost",
        int(host_port),
        "readonly_user",
        "readonly_pass",
        "readonly_db",
    )
    yield {
        "container": container,
        "host": "localhost",
        "port": int(host_port),
        "user": "readonly_user",
        "password": "readonly_pass",
        "database": "readonly_db",
    }
    try:
        container.stop(timeout=10)
        container.remove()
    except Exception as e:
        test_logger.warning(f"Failed to remove readonly container: {e}")


@pytest.fixture
def pg_connector(postgres_container):
    """Создает PGConnector для тестового контейнера."""

    return PGConnector(
        host=postgres_container["host"],
        port=postgres_container["port"],
        user=postgres_container["user"],
        password=postgres_container["password"],
        dbname=postgres_container["database"],
    )


@pytest.fixture
def readonly_pg_connector(readonly_postgres_container):
    """Создает PGConnector для read-only контейнера."""

    return PGConnector(
        host=readonly_postgres_container["host"],
        port=readonly_postgres_container["port"],
        user=readonly_postgres_container["user"],
        password=readonly_postgres_container["password"],
        dbname=readonly_postgres_container["database"],
    )


@pytest.fixture
def dumper(pg_connector):
    """Создает экземпляр PGPackDumper в PROD режиме."""

    return PGPackDumper(
        connector=pg_connector,
        compression_method=CompressionMethod.ZSTD,
        compression_level=CompressionLevel.ZSTD_DEFAULT,
        dump_format=DumpFormat.BINARY,
        mode=DumperMode.PROD,
        logger=test_logger,
    )


@pytest.fixture
def test_table(dumper):
    """Создает тестовую таблицу без автоинкрементного ID."""

    table_name = f"test_table_{random_string()}"
    with dumper.cursor as cur:
        cur.execute(f"""
            CREATE TABLE {table_name} (
                name VARCHAR(100),
                age INTEGER
            )
        """)
        dumper.connect.commit()
    dumper.refresh()
    yield table_name
    try:
        dumper.refresh()
        with dumper.cursor as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            dumper.connect.commit()
    except Exception as e:
        test_logger.warning(f"Failed to drop table {table_name}: {e}")


@pytest.fixture
def test_data():
    """Генерирует тестовые данные."""

    return [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
        {"name": "Diana", "age": 28},
        {"name": "Eve", "age": 32},
    ]


@pytest.fixture
def test_rows():
    """Генерирует тестовые данные в виде кортежей (для BINARY формата)."""

    return [
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35),
        ("Diana", 28),
        ("Eve", 32),
    ]


@pytest.fixture
def test_dicts():
    """Генерирует тестовые данные в виде словарей (для CSV формата)."""

    return [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
        {"name": "Diana", "age": 28},
        {"name": "Eve", "age": 32},
    ]


class FakeReader:
    """Фейковый ридер для эмуляции чтения данных."""

    def __init__(self, data=None):
        self._data = data or [("fake1", 1), ("fake2", 2)]
        self._position = 0
        self.columns = ["name", "age"]
        self.dtypes = ["str", "int"]
        self.fileobj = BytesIO(b"fake_data")

    def to_bytes(self) -> Generator[bytes, None, None]:

        yield b"fake_bytes_data"

    def to_rows(self) -> Generator[Any, None, None]:

        for row in self._data:
            yield row

    def to_pandas(self):

        return pd.DataFrame(self._data, columns=self.columns)

    def to_polars(self):

        return pl.DataFrame(self._data, schema=self.columns)

    def tell(self) -> int:

        return self._position

    def close(self) -> None:

        pass


class FakeDumper(BaseDumper):
    """
    Фейковый дампер для эмуляции чтения и записи данных.
    Реализует все абстрактные методы BaseDumper.
    """

    def __init__(self, mode: DumperMode = DumperMode.TEST):
        self.mode = mode
        self.connector = None
        self.logger = test_logger
        self.dump_format = DumpFormat.BINARY
        self.s3_file = False
        self.with_compression = False
        self.is_between = False
        self._written_data = []
        self._read_data = None
        self._compression_method = None
        self._compression_level = None
        self._timeout = 30
        self._isolation = None
        self._dbname = "fake_db"
        self._version = "15.0"

    @property
    def stream_type(self) -> str:
        return self.dump_format.name.lower()

    def write_between(
        self,
        table_dest: str,
        table_src: str = None,
        query_src: str = None,
        dumper_src: object = None,
    ) -> None:
        _ = dumper_src
        self._written_data.append(
            {
                "operation": "write_between",
                "table_dest": table_dest,
                "table_src": table_src,
                "query_src": query_src,
            }
        )

    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
        table_name: str,
        source: object = None,
        destination: object = None,
    ) -> None:
        _ = source, destination
        data = b"".join(bytes_data).decode("utf-8")
        self._written_data.append(
            {
                "operation": "from_bytes",
                "table_name": table_name,
                "data": data[:100],
            }
        )

    def from_rows(
        self,
        dtype_data: Iterable[Any],
        table_name: str,
        source: object = None,
    ) -> None:
        _ = source
        rows_count = (
            len(list(dtype_data)) if hasattr(dtype_data, "__len__") else 0
        )
        self._written_data.append(
            {
                "operation": "from_rows",
                "table_name": table_name,
                "rows_count": rows_count,
            }
        )

    def metadata(
        self,
        query: str = None,
        table_name: str = None,
        reader_meta: bool = False,
    ):
        """Возвращает метаданные."""

        self._written_data.append(
            {
                "operation": "metadata",
                "reader_meta": reader_meta,
                "query": query,
                "table_name": table_name,
            }
        )

        if reader_meta:
            return b"fake_internal_metadata"

        return DBMetadata(
            name=self._dbname,
            version=self._version,
            columns={"name": "text", "age": "int4"},
        )

    def _dbmeta(self, metadata):  # noqa: ARG002
        return self.metadata()

    def read_dump(
        self,
        fileobj,
        query: str = None,
        table_name: str = None,
    ) -> None:
        _ = fileobj
        self._written_data.append(
            {
                "operation": "read_dump",
                "table_name": table_name,
                "query": query,
            }
        )

    def write_dump(
        self,
        fileobj,
        table_name: str,
    ) -> None:
        _ = fileobj
        self._written_data.append(
            {"operation": "write_dump", "table_name": table_name}
        )

    def refresh(self) -> None:
        self._written_data.append({"operation": "refresh"})

    def close(self) -> None:
        self._written_data.append({"operation": "close"})

    def to_bytes(self) -> Iterator[bytes]:
        self._written_data.append({"operation": "to_bytes"})
        if self._read_data:
            yield self._read_data.encode("utf-8")
        else:
            yield b"fake_data_from_table"

    def to_reader(
        self,
        query: str = None,
        table_name: str = None,
        metadata: object = None,
    ):
        _ = metadata
        self._written_data.append(
            {
                "operation": "to_reader",
                "query": query,
                "table_name": table_name,
            }
        )
        return FakeReader()

    def to_fileobj(
        self,
        query: str = None,
        table_name: str = None,
        compression_method: object = None,
        do_compress_action: object = False,
    ):
        _ = compression_method, do_compress_action
        self._written_data.append(
            {
                "operation": "to_fileobj",
                "query": query,
                "table_name": table_name,
            }
        )
        return BytesIO(b"fake_data")

    def _to_fileobj(
        self,
        query: str = None,
        table_name: str = None,
        metadata: DBMetadata = None,
    ):
        _ = query, table_name, metadata
        return BytesIO(b"fake_data")

    def set_read_data(self, data: str):
        self._read_data = data

    def get_written_data(self):
        return self._written_data

    def clear_written_data(self):
        self._written_data.clear()


@pytest.fixture
def fake_dumper():
    """Фикстура для фейкового дампера."""

    return FakeDumper()
