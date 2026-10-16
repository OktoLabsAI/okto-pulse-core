"""Regression for Windows failures before parametrized test bodies run."""

from test_logging import test_log_filename as filename


def test_parametrized_ids_are_writable_and_distinct(tmp_path):
    ids = [
        r"tests/a.py::test_x[from importlib.metadata import version\n]",
        'tests/a.py::test_x[<lambda>:"C:\\data"|*?]',
        "CON", "test/" + "long" * 200,
        "test/a", "test:a", "test\\a",
    ]
    paths = [tmp_path / filename(test_id) for test_id in ids]
    assert len(set(paths)) == len(ids)
    for index, path in enumerate(paths):
        assert len(path.name) <= 126
        path.write_text(str(index), encoding="utf-8")
        assert path.read_text(encoding="utf-8") == str(index)
