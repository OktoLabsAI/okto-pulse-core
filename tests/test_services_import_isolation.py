"""Fresh-process regressions for the public Core services package."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import textwrap
from repository_checkout_testing import community_repo_for


CORE_SRC = Path(__file__).resolve().parents[1] / "src"


def _run_fresh_core_import(
    script: str, *, cwd: Path
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join((str(CORE_SRC), str(community_repo_for(CORE_SRC.parent) / "src")))
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_current_quality_port_import_is_fresh_and_services_main_is_lazy(
    tmp_path: Path,
) -> None:
    result = _run_fresh_core_import(
        """
        import sys

        from importlib.util import find_spec
        assert find_spec("okto_pulse.core.ports.requirement_lint") is None
        from okto_pulse.core.ports import quality_assessment

        assert quality_assessment.QualityAssessmentPersistencePort is not None
        assert "okto_pulse.core.services.main" not in sys.modules

        from okto_pulse.core.services import AgentService

        assert AgentService.__module__ == "okto_pulse.core.services.main"
        """,
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr


def test_every_public_service_export_resolves_without_duplicates(
    tmp_path: Path,
) -> None:
    result = _run_fresh_core_import(
        """
        from okto_pulse.core import services
        from okto_pulse.core.services.governance_observability import (
            build_qa_self_answer_denied_details,
        )

        assert len(services.__all__) == len(set(services.__all__))
        resolved = {name: getattr(services, name) for name in services.__all__}
        assert set(resolved) == set(services.__all__)
        assert (
            resolved["build_safe_qa_self_answer_denied_details"]
            is build_qa_self_answer_denied_details
        )
        """,
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr
