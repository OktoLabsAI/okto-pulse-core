from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path


REAL_TEST = Path(__file__).with_name("test_kg_real_integration.py")


def test_real_kg_test_has_no_hardcoded_home_or_board_identity() -> None:
    source = REAL_TEST.read_text(encoding="utf-8")
    tree = ast.parse(source)
    string_literals = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }

    assert "~/.okto-pulse" not in string_literals
    assert not any(
        value.count("-") == 4 and len(value) == 36 for value in string_literals
    )


def test_real_kg_collection_is_fail_closed_without_all_three_variables(
    tmp_path: Path,
) -> None:
    environment = os.environ.copy()
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"

    script = textwrap.dedent(
        f"""
        import json
        import os
        import runpy
        import ladybug

        constructor_count = 0
        def forbidden_database(*args, **kwargs):
            global constructor_count
            constructor_count += 1
            raise AssertionError("Ladybug Database must remain inert")
        ladybug.Database = forbidden_database

        complete = {{
            "OKTO_PULSE_REAL_KG": "1",
            "OKTO_PULSE_REAL_KG_HOME": {str(tmp_path / 'isolated-home')!r},
            "OKTO_PULSE_REAL_KG_BOARD_ID": "isolated-board",
        }}
        outcomes = []
        for index, missing in enumerate(complete):
            for key, value in complete.items():
                if key == missing:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            namespace = runpy.run_path(
                {str(REAL_TEST)!r},
                run_name=f"real_kg_fence_probe_{{index}}",
            )
            target = namespace["_REAL_TARGET"]
            outcomes.append({{
                "missing": missing,
                "configured": target.configured,
                "target_exists": namespace["_real_kuzu_exists"](),
            }})
        print(json.dumps({{
            "constructor_count": constructor_count,
            "outcomes": outcomes,
        }}, sort_keys=True))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["constructor_count"] == 0
    assert len(payload["outcomes"]) == 3
    assert all(not item["configured"] for item in payload["outcomes"])
    assert all(not item["target_exists"] for item in payload["outcomes"])
    assert not (tmp_path / "isolated-home").exists()


def test_real_kg_graph_is_always_opened_read_only() -> None:
    source = REAL_TEST.read_text(encoding="utf-8")
    tree = ast.parse(source)
    database_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "Database"
    ]

    assert database_calls
    for call in database_calls:
        read_only = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "read_only"),
            None,
        )
        assert isinstance(read_only, ast.Constant) and read_only.value is True


def test_real_kg_target_uses_the_community_ladybug_filename() -> None:
    source = REAL_TEST.read_text(encoding="utf-8")

    assert '"graph.lbug"' in source
    assert '"graph.kuzu"' not in source


def test_explicit_real_kg_target_opens_once_read_only_and_restores_environment(
    tmp_path: Path,
) -> None:
    data_home = tmp_path / "explicit-real-home"
    board_id = "isolated-board"
    database = data_home / "data" / "pulse.db"
    graph = data_home / "boards" / board_id / "graph.lbug"
    database.parent.mkdir(parents=True)
    database.touch()
    graph.mkdir(parents=True)

    environment = os.environ.copy()
    environment.update(
        {
            "OKTO_PULSE_REAL_KG": "1",
            "OKTO_PULSE_REAL_KG_HOME": str(data_home),
            "OKTO_PULSE_REAL_KG_BOARD_ID": board_id,
            "DATABASE_URL": "before-database-url",
            "KG_BASE_DIR": "before-kg-base-dir",
        }
    )
    script = textwrap.dedent(
        f"""
        import json
        import os
        import runpy

        namespace = runpy.run_path(
            {str(REAL_TEST)!r},
            run_name="real_kg_explicit_probe",
        )
        calls = []
        writes = []

        class FakeDatabase:
            def __init__(self, path, **kwargs):
                calls.append({{"path": path, **kwargs}})
            def close(self):
                return None

        class FakeConnection:
            def __init__(self, database):
                self.database = database
            def execute(self, statement, *args, **kwargs):
                writes.append(statement)
                raise AssertionError("No query is expected in the open/close probe")
            def close(self):
                return None

        namespace["kuzu"].Database = FakeDatabase
        namespace["kuzu"].Connection = FakeConnection
        database, connection = namespace["_open_real_kuzu"]()
        namespace["_close_kuzu"](database, connection)

        fixture = namespace["_real_env"].__wrapped__()
        next(fixture)
        during = {{
            "DATABASE_URL": os.environ.get("DATABASE_URL"),
            "KG_BASE_DIR": os.environ.get("KG_BASE_DIR"),
        }}
        try:
            next(fixture)
        except StopIteration:
            pass
        after = {{
            "DATABASE_URL": os.environ.get("DATABASE_URL"),
            "KG_BASE_DIR": os.environ.get("KG_BASE_DIR"),
        }}
        print(json.dumps({{
            "calls": calls,
            "writes": writes,
            "during": during,
            "after": after,
        }}, sort_keys=True))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert len(payload["calls"]) == 1
    assert payload["calls"][0]["path"] == str(graph)
    assert payload["calls"][0]["read_only"] is True
    assert payload["writes"] == []
    assert payload["during"]["DATABASE_URL"].endswith(str(database))
    assert payload["during"]["KG_BASE_DIR"] == str(data_home)
    assert payload["after"] == {
        "DATABASE_URL": "before-database-url",
        "KG_BASE_DIR": "before-kg-base-dir",
    }
