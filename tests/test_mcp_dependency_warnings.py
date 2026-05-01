import os
import subprocess
import sys
from pathlib import Path


def test_mcp_server_import_does_not_emit_fastmcp_authlib_deprecation_warning():
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get("PYTHONPATH", "")

    completed = subprocess.run(
        [
            sys.executable,
            "-W",
            "always",
            "-c",
            "import okto_pulse.core.mcp.server; print('import ok')",
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert "import ok" in completed.stdout
    assert "AuthlibDeprecationWarning" not in completed.stderr
    assert "authlib.jose module is deprecated" not in completed.stderr
