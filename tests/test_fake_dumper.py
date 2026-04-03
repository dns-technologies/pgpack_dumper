def test_fake_dumper_write_between(fake_dumper):
    """Тестирует эмуляцию write_between."""

    fake_dumper.write_between("dest_table", "src_table")
    written = fake_dumper.get_written_data()
    assert len(written) == 1  # noqa: S101
    assert written[0]["operation"] == "write_between"  # noqa: S101
    assert written[0]["table_dest"] == "dest_table"  # noqa: S101


def test_fake_dumper_from_bytes(fake_dumper):
    """Тестирует эмуляцию записи из байтов."""

    test_data = [b"row1,data\n", b"row2,data\n"]
    fake_dumper.from_bytes(test_data, "test_table")
    written = fake_dumper.get_written_data()
    assert len(written) == 1  # noqa: S101
    assert written[0]["operation"] == "from_bytes"  # noqa: S101
    assert written[0]["table_name"] == "test_table"  # noqa: S101


def test_fake_dumper_from_rows(fake_dumper):
    """Тестирует эмуляцию записи из строк."""

    test_rows = [("Alice", 30), ("Bob", 25)]
    fake_dumper.from_rows(test_rows, "test_table")
    written = fake_dumper.get_written_data()
    assert len(written) == 1  # noqa: S101
    assert written[0]["operation"] == "from_rows"  # noqa: S101
    assert written[0]["table_name"] == "test_table"  # noqa: S101
