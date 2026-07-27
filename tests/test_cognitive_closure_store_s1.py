"""S1 / card 9aeeaebd — evolução do CognitiveConsolidationItemStore/Item +
normalização de artifact_id.

Cenários cobertos (Cognitive Closure S1, spec 2012f38d):
  * ts_af3b6dca (test card 6b038087) — ``normalize_cognitive_artifact_id``
    reconcilia ``card:<uuid>`` e ``bug:<uuid>`` no MESMO ``artifact_id``,
    preservando ``source_ref_original`` como aliases distintos auditáveis;
    outros tipos de ref permanecem distintos; determinístico.
  * ts_2517ecd6 (test card 957c34e5) — o store evoluído lê itens ANTIGOS (sem
    ``reason_code``/``artifact_id``/``source_ref_original``) com defaults seguros
    E itens NOVOS preservando todos os campos; nenhum enum persistido novo
    (só CognitiveItemStatus/CognitivePendingOutcomeType); a leitura NÃO muta o
    disco (tr_3db366b6).
  * ``update_item`` persiste os campos novos e os PRESERVA quando o caller os
    omite numa atualização posterior (tr_3d6b29fe).
"""

from __future__ import annotations

from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
    compute_cognitive_item_id,
    normalize_cognitive_artifact_id,
    project_item_for_api,
    project_item_for_update_api,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey

UUID_A = "11111111-1111-1111-1111-111111111111"


# ---------------------------------------------------------------------------
# ts_af3b6dca — artifact_id normalization
# ---------------------------------------------------------------------------


def test_normalize_collapses_card_and_bug_alias():
    assert normalize_cognitive_artifact_id(f"card:{UUID_A}") == f"card:{UUID_A}"
    assert normalize_cognitive_artifact_id(f"bug:{UUID_A}") == f"card:{UUID_A}"
    # par card:/bug: do mesmo uuid → MESMO artifact_id (reconciliação, ac_50e4d48e)
    assert normalize_cognitive_artifact_id(
        f"card:{UUID_A}"
    ) == normalize_cognitive_artifact_id(f"bug:{UUID_A}")


def test_normalize_is_case_insensitive_on_uuid():
    assert normalize_cognitive_artifact_id(f"BUG:{UUID_A.upper()}") == f"card:{UUID_A}"


def test_normalize_keeps_other_types_distinct():
    assert normalize_cognitive_artifact_id(f"spec:{UUID_A}") == f"spec:{UUID_A}"
    assert normalize_cognitive_artifact_id(
        f"spec:{UUID_A}"
    ) != normalize_cognitive_artifact_id(f"card:{UUID_A}")


def test_normalize_preserves_non_uuid_refs_and_is_deterministic():
    assert normalize_cognitive_artifact_id("decision:spec-1:local-9") == (
        "decision:spec-1:local-9"
    )
    assert normalize_cognitive_artifact_id("weird") == "weird"
    assert normalize_cognitive_artifact_id("") == ""
    # determinístico
    assert normalize_cognitive_artifact_id(f"bug:{UUID_A}") == (
        normalize_cognitive_artifact_id(f"bug:{UUID_A}")
    )


def test_item_aliases_share_artifact_id_with_distinct_originals():
    card = CognitiveConsolidationItem(
        item_id="i1", board_id="b", kg_generation_id="g",
        source_ref=f"card:{UUID_A}", artifact_type="card",
        status=CognitiveItemStatus.PENDING.value, recorded_at="t",
    )
    bug = CognitiveConsolidationItem(
        item_id="i2", board_id="b", kg_generation_id="g",
        source_ref=f"bug:{UUID_A}", artifact_type="bug",
        status=CognitiveItemStatus.PENDING.value, recorded_at="t",
    )
    assert card.artifact_id == bug.artifact_id == f"card:{UUID_A}"
    # aliases auditáveis distintos preservados
    assert card.source_ref_original == f"card:{UUID_A}"
    assert bug.source_ref_original == f"bug:{UUID_A}"


# ---------------------------------------------------------------------------
# Helpers de seed do ledger file-backed
# ---------------------------------------------------------------------------


def _write_record(store, board_id, gen, items):
    record = {
        "board_id": board_id,
        "kg_generation_id": gen,
        "pending_count": len(items),
        "pending_refs": sorted({it["source_ref"] for it in items}),
        "status": "pending",
        "recorded_at": "2026-06-17T00:00:00+00:00",
        "items": items,
    }
    key = RebuildAuditKey(
        namespace="cognitive_pending",
        board_id=board_id,
        kg_generation_id=gen,
    )
    store.artifact_store.write_json_atomic(key, record)
    return key


# ---------------------------------------------------------------------------
# ts_2517ecd6 — store lê item antigo (defaults seguros) e novo (campos completos)
# ---------------------------------------------------------------------------


def test_store_reads_legacy_and_new_items_without_disk_mutation(tmp_path):
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    board, gen = "b1", "gen-1"
    legacy = {  # SEM reason_code/artifact_id/source_ref_original
        "item_id": "leg",
        "board_id": board,
        "kg_generation_id": gen,
        "source_ref": f"card:{UUID_A}",
        "artifact_type": "card",
        "status": CognitiveItemStatus.PENDING.value,
        "recorded_at": "2026-06-17T00:00:00+00:00",
    }
    new = {  # COM todos os campos novos
        "item_id": "new",
        "board_id": board,
        "kg_generation_id": gen,
        "source_ref": f"bug:{UUID_A}",
        "artifact_type": "bug",
        "status": CognitiveItemStatus.CONSOLIDATED.value,
        "recorded_at": "2026-06-17T00:00:00+00:00",
        "outcome_type": CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value,
        "reason_code": "no_action_required",
        "justification": "nothing to consolidate",
        "actor": "agent-x",
        "revisit_at": "2026-07-01T00:00:00+00:00",
        "source_ref_original": f"bug:{UUID_A}",
        "artifact_id": f"card:{UUID_A}",
        "evidence_refs": ["e1", "e2"],
    }
    key = _write_record(store, board, gen, [legacy, new])
    before = store.artifact_store.read_json(key)

    items = {i.item_id: i for i in store.list_items(board, gen)}

    # Item ANTIGO: defaults seguros, sem mutar o que está em disco.
    leg = items["leg"]
    assert leg.artifact_id == f"card:{UUID_A}"        # derivado da normalização
    assert leg.source_ref_original == f"card:{UUID_A}"
    assert leg.reason_code is None
    assert leg.justification is None
    assert leg.actor is None
    assert leg.revisit_at is None
    assert leg.evidence_refs == ()

    # Item NOVO: todos os campos preservados.
    nw = items["new"]
    assert nw.reason_code == "no_action_required"
    assert nw.justification == "nothing to consolidate"
    assert nw.actor == "agent-x"
    assert nw.revisit_at == "2026-07-01T00:00:00+00:00"
    assert nw.artifact_id == f"card:{UUID_A}"
    assert nw.source_ref_original == f"bug:{UUID_A}"
    assert nw.evidence_refs == ("e1", "e2")

    # Reconciliação: antigo (card:) e novo (bug:) caem no MESMO artifact_id.
    assert leg.artifact_id == nw.artifact_id

    # ac_86d6cd67 — nenhum enum persistido novo: status/outcome_type continuam
    # dentro dos enums existentes.
    assert {leg.status, nw.status} <= {s.value for s in CognitiveItemStatus}
    assert nw.outcome_type in {o.value for o in CognitivePendingOutcomeType}

    # tr_3db366b6 — a leitura NÃO reescreve o registro persistido.
    assert store.artifact_store.read_json(key) == before


# ---------------------------------------------------------------------------
# update_item — persiste e preserva os campos novos
# ---------------------------------------------------------------------------


def test_update_item_persists_and_preserves_new_fields(tmp_path):
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    board, gen = "b2", "gen-2"
    src = f"bug:{UUID_A}"
    iid = compute_cognitive_item_id(board, gen, src)
    seed = {
        "item_id": iid,
        "board_id": board,
        "kg_generation_id": gen,
        "source_ref": src,
        "artifact_type": "bug",
        "status": CognitiveItemStatus.PENDING.value,
        "recorded_at": "2026-06-17T00:00:00+00:00",
    }
    _write_record(store, board, gen, [seed])

    # 1ª atualização: seta os campos novos.
    upd = store.update_item(
        board_id=board,
        kg_generation_id=gen,
        item_id=iid,
        new_status=CognitiveItemStatus.SKIPPED.value,
        updated_by_agent_id="agent-1",
        reason_code="revisit_required",
        justification="needs more evidence",
        actor="agent-1",
        revisit_at="2026-07-01T00:00:00+00:00",
    )
    assert upd is not None
    assert upd.reason_code == "revisit_required"
    assert upd.justification == "needs more evidence"
    assert upd.revisit_at == "2026-07-01T00:00:00+00:00"
    assert upd.artifact_id == f"card:{UUID_A}"        # canonical persistido
    assert upd.source_ref_original == src

    # 2ª atualização: NÃO passa os campos novos → têm que ser PRESERVADOS.
    upd2 = store.update_item(
        board_id=board,
        kg_generation_id=gen,
        item_id=iid,
        new_status=CognitiveItemStatus.IN_PROGRESS.value,
        updated_by_agent_id="agent-2",
    )
    assert upd2 is not None
    assert upd2.reason_code == "revisit_required"     # preservado (tr_3d6b29fe)
    assert upd2.justification == "needs more evidence"
    assert upd2.actor == "agent-1"
    assert upd2.revisit_at == "2026-07-01T00:00:00+00:00"
    assert upd2.artifact_id == f"card:{UUID_A}"

    # Persistido em disco: reler do store confirma.
    reread = {i.item_id: i for i in store.list_items(board, gen)}[iid]
    assert reread.reason_code == "revisit_required"
    assert reread.revisit_at == "2026-07-01T00:00:00+00:00"
    assert reread.artifact_id == f"card:{UUID_A}"


# ---------------------------------------------------------------------------
# Rework S1.1 (F1) — as projeções API/MCP ecoam o reason_code persistido
# ---------------------------------------------------------------------------


def test_projections_echo_persisted_reason_code(tmp_path):
    """Rework F1: project_item_for_api e project_item_for_update_api projetam
    item.reason_code (não mais hardcode None) quando o store o carrega; item
    legacy sem reason_code continua projetando None; e a shape API NÃO expõe
    artifact_id/source_ref_original (carry-forward do S3 Action Center)."""
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    board, gen = "b3", "gen-3"
    src = f"bug:{UUID_A}"
    iid = compute_cognitive_item_id(board, gen, src)
    _write_record(store, board, gen, [{
        "item_id": iid,
        "board_id": board,
        "kg_generation_id": gen,
        "source_ref": src,
        "artifact_type": "bug",
        "status": CognitiveItemStatus.PENDING.value,
        "recorded_at": "2026-06-17T00:00:00+00:00",
    }])

    updated = store.update_item(
        board_id=board,
        kg_generation_id=gen,
        item_id=iid,
        new_status=CognitiveItemStatus.SKIPPED.value,
        updated_by_agent_id="agent-1",
        reason_code="revisit_required",
    )
    assert updated is not None
    # ambas as projeções ecoam o reason_code armazenado
    assert project_item_for_api(updated)["reason_code"] == "revisit_required"
    assert project_item_for_update_api(updated)["reason_code"] == "revisit_required"

    # item legacy/sem reason_code → projeção continua None (nunca inventado)
    legacy = CognitiveConsolidationItem(
        item_id="leg",
        board_id=board,
        kg_generation_id=gen,
        source_ref=f"card:{UUID_A}",
        artifact_type="card",
        status=CognitiveItemStatus.PENDING.value,
        recorded_at="t",
    )
    assert legacy.reason_code is None
    assert project_item_for_api(legacy)["reason_code"] is None
    assert project_item_for_update_api(legacy)["reason_code"] is None

    # carry-forward S3 — a shape API/MCP NÃO expõe artifact_id/source_ref_original
    # agora (extensão de contrato fica no Action Center/read-model).
    api_shape = project_item_for_api(updated)
    assert "artifact_id" not in api_shape
    assert "source_ref_original" not in api_shape
