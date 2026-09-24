"""F4: startup remains composable while tuning/effect writers cannot return."""
from pathlib import Path
import pytest

from okto_pulse.core.application.boundary.conformance_suite import settings_split_conformance
from okto_pulse.core.ports.relational_services import register_runtime_settings_adapter
from okto_pulse.core.services import settings_service
from okto_pulse.core.runtime_context import RuntimeValueRegistry, runtime_value_scope


@pytest.mark.asyncio
async def test_startup_forwards_once_to_edition_without_edit_or_scheduler_capability():
    calls = []
    class StartupOnly:
        async def apply_persisted_settings_to_core_settings(self):
            calls.append("startup")
            return {"kg_queue_alert_threshold": 1000, "kg_grafx_options": {}}
        def __getattr__(self, name):
            pytest.fail(f"Unexpected startup capability: {name}")
    with runtime_value_scope(RuntimeValueRegistry()):
        register_runtime_settings_adapter(StartupOnly())
        assert await settings_service.apply_persisted_settings_to_core_settings() == {
            "kg_queue_alert_threshold": 1000, "kg_grafx_options": {},
        }
    assert calls == ["startup"]
    for name in ("get_runtime_settings", "put_runtime_settings", "_apply_live_tick_settings", "apply_tick_runtime_effects"):
        assert not hasattr(settings_service, name)


def test_startup_boundary_gate_passes_real_facade():
    report = settings_split_conformance()
    assert report.status == "baseline", report.evidence
    assert all(report.evidence["checks"].values())


@pytest.mark.parametrize("extra", [
    "def put_runtime_settings(values): return values",
    "async def renamed_writer(values): return values",
    "put_runtime_settings = lambda values: values",
    "put_runtime_settings = apply_persisted_settings_to_core_settings",
    "from okto_pulse.core.kg.scheduler_singleton import get_scheduler",
    "configure_settings({})",
])
def test_startup_boundary_rejects_hidden_writers_aliases_and_effects(tmp_path, extra):
    source = Path(settings_service.__file__).read_text(encoding="utf-8")
    path = tmp_path / "okto_pulse/core/services/settings_service.py"
    path.parent.mkdir(parents=True)
    path.write_text(source + "\n" + extra + "\n", encoding="utf-8")
    assert settings_split_conformance(tmp_path).status == "blocking"
