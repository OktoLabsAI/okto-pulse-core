"""Tests for okto_pulse.core.mcp.helpers.parse_multi_value (ideação 75d4b2a3).

Cobre os 14 ACs da spec 6a2b02ab mais o audit grep que garante que
``server.py`` não carregue mais nenhum ``split("|")`` após o refactor.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from okto_pulse.core.mcp.helpers import parse_multi_value


# ===========================================================================
# AC-1 a AC-4: entradas nulas / vazias / não-string
# ===========================================================================


def test_none_returns_empty_list():
    assert parse_multi_value(None) == []


def test_empty_string_returns_empty_list():
    assert parse_multi_value("") == []


def test_whitespace_only_returns_empty_list():
    assert parse_multi_value("   \t \n  ") == []


def test_non_string_returns_empty_list():
    assert parse_multi_value(123) == []  # type: ignore[arg-type]
    assert parse_multi_value(["a", "b"]) == ["a", "b"]


# ===========================================================================
# AC-5 a AC-8: caminho pipe (legado)
# ===========================================================================


def test_pipe_basic_split():
    assert parse_multi_value("a|b|c") == ["a", "b", "c"]


def test_pipe_strips_whitespace_per_item():
    assert parse_multi_value("  a | b |  c  ") == ["a", "b", "c"]


def test_pipe_drops_empty_items():
    assert parse_multi_value("a||b|") == ["a", "b"]
    assert parse_multi_value("|a|") == ["a"]


def test_pipe_expands_legacy_backslash_n_escape():
    # Escape literal de duas letras (\n em texto) vira newline real — mantém
    # bit-a-bit o comportamento do helper _split antigo.
    raw = "linha1\\nlinha2|outro"
    assert parse_multi_value(raw) == ["linha1\nlinha2", "outro"]


# ===========================================================================
# AC-9 a AC-12: caminho JSON
# ===========================================================================


def test_json_array_basic():
    assert parse_multi_value('["a","b","c"]') == ["a", "b", "c"]


def test_json_array_with_surrounding_whitespace():
    assert parse_multi_value('   ["a", "b"]   ') == ["a", "b"]


def test_json_array_preserves_literal_pipe_inside_string():
    # O motivo de existir deste caminho: poder passar o caractere pipe
    # dentro de um item sem ser fatiado.
    raw = '["raw: str | None", "outro item"]'
    assert parse_multi_value(raw) == ["raw: str | None", "outro item"]


def test_json_array_strips_items_and_drops_empty():
    raw = '["  a  ", "", "  ", "b"]'
    assert parse_multi_value(raw) == ["a", "b"]


# ===========================================================================
# AC-13 a AC-14: erros do caminho JSON
# ===========================================================================


def test_json_malformed_raises_value_error():
    with pytest.raises(ValueError) as excinfo:
        parse_multi_value("[not valid json")
    assert "malformed JSON" in str(excinfo.value)


def test_json_dict_input_falls_through_to_pipe_path():
    # `{"a": 1}` não começa com `[`, então o autodetect manda para o
    # caminho pipe — não há `|`, então vira uma lista com um único item.
    # Comportamento documentado (autodetect por `[` inicial).
    assert parse_multi_value('{"a": 1}') == ['{"a": 1}']


def test_json_non_string_item_raises_value_error():
    with pytest.raises(ValueError) as excinfo:
        parse_multi_value('["ok", 42, "tail"]')
    msg = str(excinfo.value)
    assert "expected string items" in msg
    assert "int" in msg
    assert "index 1" in msg


# ===========================================================================
# Regressão: o bug original (FR-2 da spec)
# ===========================================================================


def test_regression_pipe_bug_python_type_hint_is_preserved_via_json():
    # Antes: "raw: str | None" via pipe split gerava 2 itens.
    # Agora: via JSON array, o pipe é preservado dentro do item.
    raw = '["raw: str | None"]'
    out = parse_multi_value(raw)
    assert len(out) == 1
    assert out[0] == "raw: str | None"


# ===========================================================================
# AC de auditoria: server.py não carrega mais nenhum split("|")
# ===========================================================================


def test_server_py_has_no_remaining_split_pipe():
    server_path = (
        Path(__file__).parent.parent
        / "src" / "okto_pulse" / "core" / "mcp" / "server.py"
    )
    content = server_path.read_text(encoding="utf-8")
    # Regex em vez de `in` para evitar flagrar comentários que
    # mencionem a sequência dentro de strings.
    matches = re.findall(r'\.split\("\|"\)', content)
    assert matches == [], (
        f"server.py ainda contém {len(matches)} ocorrências de "
        f'.split("|") — o refactor da ideação 75d4b2a3 não está '
        f"completo."
    )
