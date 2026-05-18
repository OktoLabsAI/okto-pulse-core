"""MCP schema generator — runtime Pydantic v2 introspection.

Spec P2 (ddbd4724): zero drift modelo<->doc via runtime generation.
Politica em 3 fases para campos sem description:
  Adocao (0-90%):   warning + placeholder
  Transicao (90-100%): CI passa com warning
  Enforcement (100%): CI fail (futuro, opt-in)
"""
from __future__ import annotations

import warnings
from typing import Any, Type

from pydantic import BaseModel

_PLACEHOLDER = "[no description — annotate Field()]"
_WARNING_TEMPLATE = "[schema_generator] Campo '{name}' em {model} sem description"


def generate_tool_schema(
    model: Type[BaseModel],
    summary: str,
    invariants: str = "",
) -> dict[str, Any]:
    """Gera JSON Schema MCP-compativel a partir de modelo Pydantic v2.

    Percorre cada campo do modelo e emite warning para campos sem description.
    Campos sem description recebem placeholder para nao quebrar o schema.

    Args:
        model: Classe Pydantic v2 a ser introspectada.
        summary: Titulo curto da tool MCP (vai para ``title``).
        invariants: Restricoes invariantes da tool (vai para ``description``).

    Returns:
        dict com title, description (invariants), type='object', properties, required.
    """
    raw_schema = model.model_json_schema()
    fields = model.model_fields

    properties: dict[str, Any] = {}
    for name, field_info in fields.items():
        prop = dict(raw_schema.get("properties", {}).get(name, {}))
        description = getattr(field_info, "description", None)
        if not description:
            warnings.warn(
                _WARNING_TEMPLATE.format(name=name, model=model.__name__),
                UserWarning,
                stacklevel=2,
            )
            description = _PLACEHOLDER
        prop["description"] = description
        properties[name] = prop

    return {
        "title": summary,
        "description": invariants,
        "type": "object",
        "properties": properties,
        "required": raw_schema.get("required", []),
    }


def audit_description_coverage(model: Type[BaseModel]) -> dict[str, Any]:
    """Audita cobertura de Field(description=...).

    Args:
        model: Classe Pydantic v2 a ser auditada.

    Returns:
        dict com {total, annotated, coverage_pct, missing}.
    """
    fields = model.model_fields
    total = len(fields)
    missing = [
        name for name, fi in fields.items() if not getattr(fi, "description", None)
    ]
    annotated = total - len(missing)
    return {
        "total": total,
        "annotated": annotated,
        "coverage_pct": round(annotated / total * 100, 1) if total else 100.0,
        "missing": missing,
    }


__all__ = ["generate_tool_schema", "audit_description_coverage"]
