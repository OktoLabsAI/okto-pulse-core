"""S2.1 / card 6eb348fb — bug closure branch sobre o store compartilhado.

Cenários (spec 6b91a862):
  * ts_c67fbe09 (test card 7507b8a4) — a branch de bug NÃO cria store/tabela/fila/
    enum próprio: usa o CognitiveConsolidationItemStore compartilhado, os enums
    existentes, e as action labels bug-specific só como DTO/campos (nunca enum
    persistido).
  * ts_b3d2a5e9 (test card f7e7c3f5) — canonical debt card:<uuid> e cognitive item
    bug:<uuid> reconciliam pelo mesmo artifact_id normalizado.

Sem stubs; lê/escreve o store real via materialize_from_marker.
"""

from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.kg.bug_cognitive_closure import (
    BugCognitiveActionLabel,
    project_bug_action_label,
)
from okto_pulse.core.kg.cognitive_closeout_gate import resolve_cognitive_source_refs
from okto_pulse.core.kg.cognitive_readiness import CognitiveReasonCode
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingOutcomeType,
    normalize_cognitive_artifact_id,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id

UUID_A = "11111111-1111-1111-1111-111111111111"


# ---------------------------------------------------------------------------
# ts_c67fbe09 — bug branch reusa store/enums; action labels só DTO
# ---------------------------------------------------------------------------


def test_bug_action_labels_are_not_persisted_enums():
    # As action labels bug-specific (DTO) são DISJUNTAS dos enums persistidos
    # (CognitiveItemStatus / CognitivePendingOutcomeType) — nenhum enum novo.
    label_values = {lbl.value for lbl in BugCognitiveActionLabel}
    persisted = (
        {s.value for s in CognitiveItemStatus}
        | {o.value for o in CognitivePendingOutcomeType}
    )
    assert label_values.isdisjoint(persisted), (
        "bug action labels não podem colidir com enums persistidos"
    )
    assert label_values == {
        "create_learning_validates_bug",
        "link_existing_learning",
        "consume_path_b_lineage",
        "no_reusable_learning",
    }


def test_project_bug_action_label_mapping():
    # consolidated com learning real → create_learning_validates_bug
    for outcome in (
        CognitivePendingOutcomeType.RELATION_CREATED.value,
        CognitivePendingOutcomeType.CANDIDATE_CREATED.value,
        CognitivePendingOutcomeType.FORMAL_DECISION_PROMOTED.value,
    ):
        assert project_bug_action_label(reason_code=None, outcome_type=outcome) == (
            BugCognitiveActionLabel.CREATE_LEARNING_VALIDATES_BUG.value
        )
    # link a learning/decision existente
    assert project_bug_action_label(
        reason_code=None,
        outcome_type=CognitivePendingOutcomeType.EXISTING_DECISION_LINKED.value,
    ) == BugCognitiveActionLabel.LINK_EXISTING_LEARNING.value
    # path_b_pending → consume_path_b_lineage
    assert project_bug_action_label(
        reason_code=CognitiveReasonCode.PATH_B_PENDING.value, outcome_type=None,
    ) == BugCognitiveActionLabel.CONSUME_PATH_B_LINEAGE.value
    # trivial/duplicate/no_reusable → no_reusable_learning (sem fabricar Learning)
    for rc in (
        CognitiveReasonCode.NO_REUSABLE_LEARNING.value,
        CognitiveReasonCode.DUPLICATE_BUG.value,
        CognitiveReasonCode.TRIVIAL_FIX.value,
    ):
        assert project_bug_action_label(reason_code=rc, outcome_type=None) == (
            BugCognitiveActionLabel.NO_REUSABLE_LEARNING.value
        )
    # nada aplicável → None (nunca label fabricada)
    assert project_bug_action_label(reason_code=None, outcome_type=None) is None
    assert project_bug_action_label(
        reason_code="root_cause_unconfirmed", outcome_type=None,
    ) is None


def test_bug_card_resolves_to_bug_source_ref():
    # lock: um Card com card_type=bug resolve para source_ref cognitivo bug:<id>.
    refs = resolve_cognitive_source_refs(
        entity_type="card",
        entity=SimpleNamespace(id=UUID_A, card_type="bug"),
        entity_id=UUID_A,
    ).source_refs
    assert refs == (f"bug:{UUID_A}",)


def test_bug_materializes_into_shared_store(tmp_path):
    # A branch de bug usa o MESMO CognitiveConsolidationItemStore — não um store
    # próprio. Materializa um bug e lê de volta com artifact_type=bug,
    # source_ref_original=bug:<uuid>, artifact_id normalizado=card:<uuid>.
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    board, gen = "bug-branch", generate_kg_generation_id()
    result = store.materialize_from_marker(
        board_id=board,
        kg_generation_id=gen,
        event_ref="evt_bug",
        source_set=[{
            "artifact_type": "bug",
            "id": UUID_A,
            "source_ref": f"bug:{UUID_A}",
            "content_hash": "h-bug",
        }],
    )
    assert result.item_count == 1

    items = store.list_items(board, gen)
    assert len(items) == 1
    item = items[0]
    assert item.artifact_type == "bug"
    assert item.source_ref == f"bug:{UUID_A}"
    assert item.source_ref_original == f"bug:{UUID_A}"
    assert item.artifact_id == f"card:{UUID_A}"  # normalizado (alias card/bug)
    assert item.status == CognitiveItemStatus.PENDING.value
    # status/outcome continuam dos enums existentes (sem enum novo de bug)
    assert item.status in {s.value for s in CognitiveItemStatus}


# ---------------------------------------------------------------------------
# ts_b3d2a5e9 — canonical debt card:<uuid> e cognitive bug:<uuid> reconciliam
# ---------------------------------------------------------------------------


def test_bug_and_card_aliases_reconcile_via_artifact_id(tmp_path):
    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    board, gen = "bug-alias", generate_kg_generation_id()
    store.materialize_from_marker(
        board_id=board,
        kg_generation_id=gen,
        event_ref="evt_alias",
        source_set=[{
            "artifact_type": "bug",
            "id": UUID_A,
            "source_ref": f"bug:{UUID_A}",
            "content_hash": "h",
        }],
    )
    cognitive_item = store.list_items(board, gen)[0]

    # canonical debt referencia o mesmo artefato como card:<uuid>.
    canonical_debt_source_ref = f"card:{UUID_A}"

    # ambos reconciliam para o MESMO artifact_id normalizado.
    assert cognitive_item.artifact_id == normalize_cognitive_artifact_id(
        canonical_debt_source_ref
    )
    assert cognitive_item.artifact_id == f"card:{UUID_A}"
    # os source_ref_original distintos permanecem auditáveis (não colapsados).
    assert cognitive_item.source_ref_original == f"bug:{UUID_A}"
    assert canonical_debt_source_ref != cognitive_item.source_ref_original
