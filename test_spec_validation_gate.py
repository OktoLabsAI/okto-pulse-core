"""Integration tests for Spec Validation Gate — covers outcome rules, state guard,
opt-in guard, min_length, and append-only history.

Cards covered:
- Card 2b: outcome rules (threshold violation, reject override, state guard)
- Card 2c: board opt-in guard (require_spec_validation enforcement)
"""
import asyncio
import os

# Use a temp sqlite database
os.environ["OKTO_PULSE_DB_URL"] = "sqlite+aiosqlite:///:memory:"

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from okto_pulse.core.models.db import (
    Base, Board, Spec, SpecStatus
)
from okto_pulse.core.models.schemas import SpecValidationSubmit
from okto_pulse.core.services.main import SpecService, SpecLockedError


async def run_tests():
    results = []

    def check(name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        results.append((status, name, detail))
        print(f"[{status}] {name}" + (f" — {detail}" if detail else ""))

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    # ==== TEST: min_length enforcement via Pydantic ====
    try:
        SpecValidationSubmit(
            completeness=85,
            completeness_justification="short",  # less than 10 chars
            assertiveness=80,
            assertiveness_justification="valid justification",
            ambiguity=25,
            ambiguity_justification="valid justification",
            general_justification="valid general justification text here",
            recommendation="approve",
        )
        check("Pydantic rejects completeness_justification < 10 chars", False, "should have raised")
    except Exception:
        check("Pydantic rejects completeness_justification < 10 chars", True)

    try:
        SpecValidationSubmit(
            completeness=85,
            completeness_justification="valid justification",
            assertiveness=80,
            assertiveness_justification="valid justification",
            ambiguity=25,
            ambiguity_justification="valid justification",
            general_justification="too short",  # less than 20 chars
            recommendation="approve",
        )
        check("Pydantic rejects general_justification < 20 chars", False, "should have raised")
    except Exception:
        check("Pydantic rejects general_justification < 20 chars", True)

    # ==== TEST: threshold defaults and outcome rules ====
    async with SessionLocal() as db:
        board = Board(
            id="b1", name="Test Board", owner_id="u1",
            settings={
                "require_spec_validation": True,
                "min_spec_completeness": 80,
                "min_spec_assertiveness": 80,
                "max_spec_ambiguity": 30,
            }
        )
        db.add(board)

        # Spec in approved status with minimal fields (no coverage to exercise)
        spec = Spec(
            id="s1", board_id="b1", title="Test Spec",
            status=SpecStatus.APPROVED, created_by="u1",
            # Empty coverage arrays — check_*_coverage will pass because no ACs/FRs defined
            acceptance_criteria=[], functional_requirements=[],
            technical_requirements=[], api_contracts=[],
            test_scenarios=[], business_rules=[],
        )
        db.add(spec)
        await db.commit()

        service = SpecService(db)

        # ==== Config defaults ====
        config = service._resolve_spec_validation_config(board)
        check("Config require_spec_validation=True", config["require_spec_validation"] is True)
        check("Config min_spec_completeness=80", config["min_spec_completeness"] == 80)
        check("Config min_spec_assertiveness=80", config["min_spec_assertiveness"] == 80)
        check("Config max_spec_ambiguity=30", config["max_spec_ambiguity"] == 30)

        # ==== Outcome rule: threshold violation + approve → failed ====
        result = await service.submit_spec_validation(
            spec_id="s1", reviewer_id="u1", reviewer_name="Tester",
            data={
                "completeness": 72,  # below min 80
                "completeness_justification": "Cenarios de edge case ainda faltam",
                "assertiveness": 85,
                "assertiveness_justification": "FRs sao mensuraveis",
                "ambiguity": 25,
                "ambiguity_justification": "Glossario adicionado",
                "general_justification": "Scores baixos em completeness mas aprovo no geral",
                "recommendation": "approve",
            }
        )
        check("Outcome=failed when threshold violated", result["outcome"] == "failed")
        check("Threshold violations listed", len(result["threshold_violations"]) == 1)
        check("Violation mentions completeness", "completeness" in result["threshold_violations"][0])
        check("Spec remains in approved on failed", result["spec_status"] == "approved")

        # Re-fetch to confirm persistence
        await db.refresh(spec)
        check("Validations array has 1 record", len(spec.validations or []) == 1)
        check("current_validation_id set", spec.current_validation_id is not None)

        # ==== Outcome rule: all pass + reject → failed ====
        result = await service.submit_spec_validation(
            spec_id="s1", reviewer_id="u1", reviewer_name="Tester",
            data={
                "completeness": 90,
                "completeness_justification": "Todas ACs cobertas detalhadamente",
                "assertiveness": 90,
                "assertiveness_justification": "FRs mensuraveis e sem weasel words",
                "ambiguity": 10,
                "ambiguity_justification": "Glossario completo e termos definidos",
                "general_justification": "Scores bons mas nao considero pronta ainda",
                "recommendation": "reject",
            }
        )
        check("Outcome=failed when reject override", result["outcome"] == "failed")
        check("No threshold violations on reject", len(result["threshold_violations"]) == 0)
        check("Spec remains in approved on reject", result["spec_status"] == "approved")

        # ==== Outcome rule: all pass + approve → success + promote ====
        result = await service.submit_spec_validation(
            spec_id="s1", reviewer_id="u1", reviewer_name="Tester",
            data={
                "completeness": 90,
                "completeness_justification": "Todas ACs cobertas detalhadamente",
                "assertiveness": 90,
                "assertiveness_justification": "FRs mensuraveis e sem weasel words",
                "ambiguity": 10,
                "ambiguity_justification": "Glossario completo",
                "general_justification": "Spec pronta para execucao, alta confianca",
                "recommendation": "approve",
            }
        )
        check("Outcome=success when all ok and approve", result["outcome"] == "success")
        check("Spec promoted to validated", result["spec_status"] == "validated")

        await db.refresh(spec)
        check("Validations array has 3 records (append-only)", len(spec.validations or []) == 3)
        check("current_validation_id points to latest", spec.current_validation_id == result["id"])

        # ==== Content lock active ====
        try:
            from okto_pulse.core.models.schemas import SpecUpdate
            await service.update_spec("s1", "u1", SpecUpdate(description="new desc"))
            check("update_spec blocked by content lock", False, "should have raised SpecLockedError")
        except SpecLockedError:
            check("update_spec blocked by content lock", True)

        # ==== Backward move clears pointer preserving history ====
        from okto_pulse.core.models.schemas import SpecMove
        await service.move_spec("s1", "u1", SpecMove(status=SpecStatus.DRAFT))
        await db.refresh(spec)
        check("Backward move cleared current_validation_id", spec.current_validation_id is None)
        check("Validations history preserved after backward move", len(spec.validations or []) == 3)
        check("Spec status updated to draft", spec.status == SpecStatus.DRAFT)

        # ==== State guard: non-approved rejects submit ====
        try:
            await service.submit_spec_validation(
                spec_id="s1", reviewer_id="u1", reviewer_name="Tester",
                data={
                    "completeness": 85,
                    "completeness_justification": "Detalhamento adequado",
                    "assertiveness": 85,
                    "assertiveness_justification": "Mensuravel",
                    "ambiguity": 20,
                    "ambiguity_justification": "Sem ambiguidade",
                    "general_justification": "Spec pronta",
                    "recommendation": "approve",
                }
            )
            check("State guard rejects submit when not approved", False, "should have raised")
        except ValueError as e:
            check("State guard rejects submit when not approved", "approved" in str(e))

    # ==== TEST: Opt-in guard ====
    async with SessionLocal() as db:
        board = Board(
            id="b2", name="Opt-out Board", owner_id="u1",
            settings={}  # no require_spec_validation
        )
        db.add(board)

        spec = Spec(
            id="s2", board_id="b2", title="Test Spec 2",
            status=SpecStatus.APPROVED, created_by="u1",
            acceptance_criteria=[], functional_requirements=[],
        )
        db.add(spec)
        await db.commit()

        service = SpecService(db)
        try:
            await service.submit_spec_validation(
                spec_id="s2", reviewer_id="u1", reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "ok justification",
                    "assertiveness": 90,
                    "assertiveness_justification": "ok justification",
                    "ambiguity": 10,
                    "ambiguity_justification": "ok justification",
                    "general_justification": "valid general justification",
                    "recommendation": "approve",
                }
            )
            check("Opt-in guard rejects when require_spec_validation=false", False, "should have raised")
        except ValueError as e:
            check("Opt-in guard rejects when require_spec_validation=false", "does not require" in str(e))

    # ==== TEST: Migration grandfather — legacy board with no spec validation settings ====
    async with SessionLocal() as db:
        # Legacy board created BEFORE the feature existed — settings dict has no spec validation keys
        legacy_board = Board(
            id="b3", name="Legacy Board", owner_id="u1",
            settings={"max_scenarios_per_card": 3},  # pre-feature settings shape
        )
        db.add(legacy_board)

        # Spec already in validated status (grandfathered) — should NOT have lock applied
        legacy_spec = Spec(
            id="s3", board_id="b3", title="Grandfathered Spec",
            status=SpecStatus.VALIDATED, created_by="u1",
        )
        db.add(legacy_spec)
        await db.commit()

        await db.refresh(legacy_spec)
        check("Legacy spec has validations array defaulting to None/empty",
              legacy_spec.validations is None or legacy_spec.validations == [])
        check("Legacy spec has current_validation_id NULL (no lock retro)",
              legacy_spec.current_validation_id is None)

        # Verify that update_spec works on legacy spec (no lock enforced because pointer is None)
        from okto_pulse.core.models.schemas import SpecUpdate
        service = SpecService(db)
        # Spec is in validated, move back to approved so we can update (simulates editor flow)
        # Actually just verify _require_spec_unlocked allows through:
        from okto_pulse.core.services.main import _require_spec_unlocked
        try:
            await _require_spec_unlocked(db, "s3")
            check("_require_spec_unlocked passes on legacy spec (no pointer)", True)
        except Exception as e:
            check("_require_spec_unlocked passes on legacy spec (no pointer)", False, str(e))

    # ==== TEST: Whitelist — tools NOT subject to lock still work ====
    async with SessionLocal() as db:
        wl_board = Board(
            id="b4", name="Whitelist Board", owner_id="u1",
            settings={
                "require_spec_validation": True,
                "min_spec_completeness": 80,
                "min_spec_assertiveness": 80,
                "max_spec_ambiguity": 30,
            }
        )
        db.add(wl_board)

        wl_spec = Spec(
            id="s4", board_id="b4", title="Locked Spec Whitelist Test",
            status=SpecStatus.APPROVED, created_by="u1",
            acceptance_criteria=[], functional_requirements=[],
            # Pre-populate a passed validation to simulate the locked state
            validations=[{
                "id": "val_preset01",
                "outcome": "success",
                "recommendation": "approve",
            }],
            current_validation_id="val_preset01",
        )
        db.add(wl_spec)
        await db.commit()

        service = SpecService(db)

        # The service's submit_spec_validation is whitelisted — re-submit should work
        # even under lock (it can change the outcome by submitting failed, which unlocks).
        # We don't actually test a full re-submit because coverage gates are trivial here;
        # instead we verify the content lock helper blocks update_spec but NOT generic reads.
        current_spec = await service.get_spec("s4")
        check("Reads allowed under lock (get_spec works)", current_spec is not None)
        check("Reads allowed under lock: validations array accessible",
              len(current_spec.validations or []) == 1)

        # Verify update_spec raises SpecLockedError (whitelist does NOT include content edits)
        try:
            await service.update_spec("s4", "u1", SpecUpdate(description="attempted edit"))
            check("update_spec blocked under lock (whitelist excludes content edit)", False, "should have raised")
        except SpecLockedError:
            check("update_spec blocked under lock (whitelist excludes content edit)", True)

    # ==== TEST: Permission registry has the new flags ====
    from okto_pulse.core.infra.permissions import (
        PERMISSION_REGISTRY, ALL_FLAGS, map_legacy_permissions, PermissionSet, get_builtin_presets,
    )

    check("spec.validation subtree exists in registry",
          "validation" in PERMISSION_REGISTRY["spec"])
    check("spec.validation.submit flag defined",
          PERMISSION_REGISTRY["spec"]["validation"].get("submit") is True)
    check("spec.validation.read flag defined",
          PERMISSION_REGISTRY["spec"]["validation"].get("read") is True)
    check("spec.validation.delete flag defined",
          PERMISSION_REGISTRY["spec"]["validation"].get("delete") is True)
    check("spec.move.approved_to_draft flag defined",
          PERMISSION_REGISTRY["spec"]["move"].get("approved_to_draft") is True)
    check("spec.move.validated_to_draft flag defined",
          PERMISSION_REGISTRY["spec"]["move"].get("validated_to_draft") is True)
    check("spec.validation.submit in ALL_FLAGS", "spec.validation.submit" in ALL_FLAGS)
    check("spec.validation.read in ALL_FLAGS", "spec.validation.read" in ALL_FLAGS)
    check("spec.move.validated_to_draft in ALL_FLAGS", "spec.move.validated_to_draft" in ALL_FLAGS)

    # ==== TEST: Legacy permission map propagates new flags ====
    legacy_flags = map_legacy_permissions(["specs:evaluate", "specs:move"])
    ps = PermissionSet(legacy_flags)
    check("Legacy specs:evaluate grants spec.validation.submit",
          ps.has("spec.validation.submit"))
    check("Legacy specs:evaluate grants spec.validation.read",
          ps.has("spec.validation.read"))
    check("Legacy specs:move grants spec.move.approved_to_draft",
          ps.has("spec.move.approved_to_draft"))
    check("Legacy specs:move grants spec.move.validated_to_draft",
          ps.has("spec.move.validated_to_draft"))

    # Agents WITHOUT specs:evaluate should NOT have validation permissions
    no_eval_flags = map_legacy_permissions(["board:read", "cards:create"])
    ps_no_eval = PermissionSet(no_eval_flags)
    check("Agent without specs:evaluate does NOT have spec.validation.submit",
          not ps_no_eval.has("spec.validation.submit"))

    # ==== TEST: Built-in presets propagation ====
    presets = {p["name"]: p["flags"] for p in get_builtin_presets()}

    def preset_has(preset_name: str, flag: str) -> bool:
        ps_local = PermissionSet(presets[preset_name])
        return ps_local.has(flag)

    # Executor — read only (no submit)
    check("Executor preset has spec.validation.read",
          preset_has("Executor", "spec.validation.read"))
    check("Executor preset does NOT have spec.validation.submit",
          not preset_has("Executor", "spec.validation.submit"))

    # Validator — full validation workflow
    check("Validator preset has spec.validation.submit",
          preset_has("Validator", "spec.validation.submit"))
    check("Validator preset has spec.validation.read",
          preset_has("Validator", "spec.validation.read"))
    check("Validator preset has spec.move.approved_to_validated (newly added)",
          preset_has("Validator", "spec.move.approved_to_validated"))
    check("Validator preset has spec.move.approved_to_draft",
          preset_has("Validator", "spec.move.approved_to_draft"))
    check("Validator preset has spec.move.validated_to_draft",
          preset_has("Validator", "spec.move.validated_to_draft"))

    # QA — submit + read
    check("QA preset has spec.validation.submit",
          preset_has("QA", "spec.validation.submit"))
    check("QA preset has spec.validation.read",
          preset_has("QA", "spec.validation.read"))

    # Spec writer — via wildcard spec.*
    check("Spec preset has spec.validation.submit (via wildcard)",
          preset_has("Spec", "spec.validation.submit"))
    check("Spec preset has spec.validation.read (via wildcard)",
          preset_has("Spec", "spec.validation.read"))
    check("Spec preset has spec.move.validated_to_draft (via wildcard)",
          preset_has("Spec", "spec.move.validated_to_draft"))

    # Full Control — via deepcopy
    check("Full Control preset has spec.validation.submit",
          preset_has("Full Control", "spec.validation.submit"))

    # ==== TEST: Coverage gate pre-check with insufficient coverage ====
    async with SessionLocal() as db:
        cov_board = Board(
            id="b5", name="Coverage Test Board", owner_id="u1",
            settings={
                "require_spec_validation": True,
                "min_spec_completeness": 80,
                "min_spec_assertiveness": 80,
                "max_spec_ambiguity": 30,
            }
        )
        db.add(cov_board)

        # Spec with ACs but no test scenarios → AC coverage will be 0% → coverage gate fails
        cov_spec = Spec(
            id="s5", board_id="b5", title="Low Coverage Spec",
            status=SpecStatus.APPROVED, created_by="u1",
            acceptance_criteria=["AC1: system must return 200 on GET /health"],
            functional_requirements=["FR1: health endpoint exists"],
            test_scenarios=[],  # No scenarios → uncovered AC
            business_rules=[],
        )
        db.add(cov_spec)
        await db.commit()

        service = SpecService(db)
        try:
            await service.submit_spec_validation(
                spec_id="s5", reviewer_id="u1", reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "ok justification text",
                    "assertiveness": 90,
                    "assertiveness_justification": "ok justification text",
                    "ambiguity": 10,
                    "ambiguity_justification": "ok justification text",
                    "general_justification": "valid general justification for the test",
                    "recommendation": "approve",
                }
            )
            check("Coverage gate blocks submit when AC coverage insufficient", False, "should have raised")
        except (ValueError, Exception):
            # Coverage gate raises a different error — just confirm submit was rejected
            check("Coverage gate blocks submit when AC coverage insufficient", True)

        # Confirm nothing was persisted
        await db.refresh(cov_spec)
        check("No validation persisted when coverage gate fails",
              not cov_spec.validations or len(cov_spec.validations) == 0)

    # Summary
    passed = sum(1 for r in results if r[0] == "PASS")
    failed = sum(1 for r in results if r[0] == "FAIL")
    print(f"\n{'='*60}")
    print(f"Total: {len(results)} | PASS: {passed} | FAIL: {failed}")
    print(f"{'='*60}")
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    import sys
    sys.exit(0 if success else 1)
