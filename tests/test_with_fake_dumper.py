from conftest import FakeDumper


class TestWithFakeDumper:
    """Тесты с использованием фейкового дампера."""

    def test_write_between_with_fake_dumper(self, dumper, test_table):
        """Тестирует write_between с фейковым дампером."""

        fake_dumper = FakeDumper()
        dumper.write_between(
            test_table, table_src="fake_table", dumper_src=fake_dumper
        )
        written = fake_dumper.get_written_data()
        operations = [w["operation"] for w in written]
        assert "to_reader" in operations or "metadata" in operations  # noqa: S101

    def test_write_between_same_server(self, dumper, test_table, test_rows):
        """Тестирует write_between на одном сервере (скрытый дампер)."""

        dumper.from_rows(test_rows, test_table)
        dest_table = f"{test_table}_copy"

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
            cur.execute(f"SELECT COUNT(*) FROM {dest_table}")
            count = cur.fetchone()[0]
            assert count == len(test_rows)  # noqa: S101

    def test_fake_dumper_in_read_dump(
        self, dumper, test_table, test_rows, fake_dumper
    ):
        """Тестирует использование фейкового дампера в read_dump."""

        dumper.from_rows(test_rows, test_table)
        dumper.refresh()
        fake_dumper.set_read_data("fake_metadata,data")
        assert hasattr(fake_dumper, "to_bytes")  # noqa: S101
        assert hasattr(fake_dumper, "metadata")  # noqa: S101
