from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.cli import main as boundary_cli
from okto_pulse.core.application.boundary.conformance_matrix import (
    build_conformance_matrix,
)
from okto_pulse.core.application.boundary.dependency_conformance import (
    audit_dependency_conformance,
)
from okto_pulse.core.application.boundary.mcp_runtime_ownership_gate import (
    run_mcp_runtime_ownership_gate,
)
from okto_pulse.core.application.boundary.packaging_ownership_gate import (
    PackagingOwnershipGate,
    PackagingOwnershipGateInput,
)
from okto_pulse.core.application.boundary.wheel_metadata import (
    read_distribution_metadata,
)


def _metadata(*requirements: str) -> bytes:
    headers = [
        "Metadata-Version: 2.4",
        "Name: okto-pulse-core",
        "Version: 0.3.0",
        *(f"Requires-Dist: {requirement}" for requirement in requirements),
        "",
    ]
    return "\n".join(headers).encode("utf-8")


def _wheel(path: Path, members: list[tuple[str, bytes]]) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in members:
            archive.writestr(name, content)
    return path


def test_reader_accepts_file_directory_and_wheel_without_extraction(
    tmp_path: Path,
) -> None:
    dist_info = tmp_path / "okto_pulse_core-0.3.0.dist-info"
    dist_info.mkdir()
    metadata = dist_info / "METADATA"
    metadata.write_bytes(_metadata("pydantic>=2.5", "PyYAML>=6"))
    wheel = _wheel(
        tmp_path / "okto_pulse_core-0.3.0-py3-none-any.whl",
        [("okto_pulse_core-0.3.0.dist-info/METADATA", metadata.read_bytes())],
    )

    file_text, file_source = read_distribution_metadata(metadata)
    directory_text, directory_source = read_distribution_metadata(dist_info)
    wheel_text, wheel_source = read_distribution_metadata(wheel)

    assert file_text == directory_text == wheel_text
    assert file_source == directory_source == f"metadata:{metadata.as_posix()}"
    assert wheel_source == (
        f"wheel:{wheel.as_posix()}!okto_pulse_core-0.3.0.dist-info/METADATA"
    )
    assert list(tmp_path.rglob("METADATA")) == [metadata]


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    [
        ("malformed_archive", "wheel_metadata_unreadable:"),
        ("missing_metadata", "wheel_metadata_not_found:"),
        ("ambiguous_metadata", "wheel_metadata_ambiguous:"),
        ("invalid_metadata", ":InvalidMetadata"),
        ("invalid_utf8", ":UnicodeDecodeError"),
    ],
)
def test_reader_fails_closed_for_invalid_wheel_metadata(
    tmp_path: Path,
    case: str,
    expected_reason: str,
) -> None:
    wheel = tmp_path / f"{case}.whl"
    if case == "malformed_archive":
        wheel.write_bytes(b"not-a-zip")
    elif case == "missing_metadata":
        _wheel(wheel, [("okto_pulse/__init__.py", b"")])
    elif case == "ambiguous_metadata":
        _wheel(
            wheel,
            [
                ("first-0.dist-info/METADATA", _metadata()),
                ("second-0.dist-info/METADATA", _metadata()),
            ],
        )
    elif case == "invalid_metadata":
        _wheel(wheel, [("broken-0.dist-info/METADATA", b"Requires-Dist: x\n")])
    else:
        _wheel(wheel, [("broken-0.dist-info/METADATA", b"\xff\xfe")])

    text, reason = read_distribution_metadata(wheel)

    assert text is None
    assert expected_reason in reason


def test_reader_rejects_traversal_member_without_writing_outside_archive(
    tmp_path: Path,
) -> None:
    wheel = _wheel(
        tmp_path / "traversal.whl",
        [("../escaped-0.dist-info/METADATA", _metadata())],
    )

    text, reason = read_distribution_metadata(wheel)

    assert text is None
    assert reason.startswith("wheel_metadata_unsafe_member:")
    assert not (tmp_path.parent / "escaped-0.dist-info").exists()
    assert list(tmp_path.iterdir()) == [wheel]


def test_all_boundary_consumers_fail_closed_on_ambiguous_wheel(
    tmp_path: Path,
) -> None:
    wheel = _wheel(
        tmp_path / "ambiguous.whl",
        [
            ("first-0.dist-info/METADATA", _metadata()),
            ("second-0.dist-info/METADATA", _metadata()),
        ],
    )
    dependency = audit_dependency_conformance(wheel_metadata_path=wheel)
    matrix = build_conformance_matrix(
        dependency_report=dependency,
        include_import_boundary=False,
        required_adapter_keys=frozenset(),
    )
    packaging = PackagingOwnershipGate().run(
        PackagingOwnershipGateInput(
            dependency_report=dependency,
            include_import_boundary=False,
        )
    )
    mcp_runtime = run_mcp_runtime_ownership_gate(wheel_metadata_path=wheel)

    assert dependency.ok is False
    assert "wheel" in dependency.surfaces_audited
    assert dependency.violations[-1].diagnostic_code == "wheel_metadata_unavailable"
    assert dependency.violations[-1].location.startswith("wheel_metadata_ambiguous:")
    assert matrix.ok is False
    assert matrix.dependency_report_ok is False
    assert any(
        row.diagnostic_code == "wheel_metadata_unavailable" for row in matrix.rows
    )
    assert packaging.ok is False
    assert packaging.matrix_ok is False
    assert any(
        row.diagnostic_code == "wheel_metadata_unavailable"
        for row in packaging.blocking
    )
    assert mcp_runtime.ok is False
    assert mcp_runtime.findings[-1].diagnostic_code == "wheel_metadata_unavailable"


def test_explicit_wheel_is_authoritative_for_removed_dependency(
    tmp_path: Path,
) -> None:
    wheel = _wheel(
        tmp_path / "removed.whl",
        [("okto_pulse_core-0.dist-info/METADATA", _metadata("asyncpg>=0.29"))],
    )

    report = audit_dependency_conformance(wheel_metadata_path=wheel)

    assert any(
        finding.surface == "wheel"
        and finding.token == "asyncpg"
        and finding.diagnostic_code == "removed_dependency_present"
        for finding in report.violations
    )
    assert all(
        finding.diagnostic_code != "stale_removed_in_wheel"
        for finding in report.warnings
    )


def test_cli_commands_accept_the_same_wheel_archive(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    wheel = _wheel(
        tmp_path / "clean.whl",
        [
            (
                "okto_pulse_core-0.3.0.dist-info/METADATA",
                _metadata("pydantic>=2.5", "PyYAML>=6"),
            )
        ],
    )

    for command in (
        "dependency-conformance",
        "conformance-matrix",
        "packaging-ownership",
        "mcp-runtime-ownership",
    ):
        assert (
            boundary_cli([command, "--wheel-metadata", str(wheel), "--format", "json"])
            == 0
        )
        capsys.readouterr()
