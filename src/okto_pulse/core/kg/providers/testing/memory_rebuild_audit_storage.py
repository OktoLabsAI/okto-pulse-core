"""In-memory rebuild/audit artifact store for core contract tests."""

from __future__ import annotations

import copy
import base64
import json
import shutil
import threading
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from okto_pulse.core.kg.interfaces.cognitive_pending_work import (
    CognitivePendingRecordRef,
    CognitivePendingWorkProvider,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditArtifactStoreResolver,
    RebuildAuditKey,
)
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef


class InMemoryRebuildAuditArtifactStore(RebuildAuditArtifactStore):
    """Thread-safe fake implementing the rebuild audit storage port."""

    def __init__(self) -> None:
        self._records: dict[
            tuple[str, str, str | None, str | None], dict[str, Any]
        ] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _identity(key: RebuildAuditKey) -> tuple[str, str, str | None, str | None]:
        return (key.namespace, key.board_id, key.kg_generation_id, key.artifact_id)

    @staticmethod
    def _matches(prefix: RebuildAuditKey, key: RebuildAuditKey) -> bool:
        return (
            key.namespace == prefix.namespace
            and key.board_id == prefix.board_id
            and (
                prefix.kg_generation_id is None
                or key.kg_generation_id == prefix.kg_generation_id
            )
            and (prefix.artifact_id is None or key.artifact_id == prefix.artifact_id)
        )

    def write_json_atomic(
        self,
        key: RebuildAuditKey,
        payload: Mapping[str, Any],
    ) -> None:
        with self._lock:
            self._records[self._identity(key)] = copy.deepcopy(dict(payload))

    def read_json(self, key: RebuildAuditKey) -> dict[str, Any] | None:
        with self._lock:
            payload = self._records.get(self._identity(key))
            return copy.deepcopy(payload) if payload is not None else None

    def exists(self, key: RebuildAuditKey) -> bool:
        with self._lock:
            return self._identity(key) in self._records

    def delete_json(self, key: RebuildAuditKey) -> bool:
        with self._lock:
            identity = self._identity(key)
            existed = identity in self._records
            self._records.pop(identity, None)
            return existed

    def list_json(self, prefix: RebuildAuditKey) -> Sequence[dict[str, Any]]:
        with self._lock:
            rows: list[dict[str, Any]] = []
            for raw_key, payload in sorted(self._records.items()):
                key = RebuildAuditKey(
                    namespace=raw_key[0],
                    board_id=raw_key[1],
                    kg_generation_id=raw_key[2],
                    artifact_id=raw_key[3],
                )
                if self._matches(prefix, key):
                    rows.append(copy.deepcopy(payload))
            return rows

    def replace_json(
        self,
        key: RebuildAuditKey,
        transform: Callable[[dict[str, Any] | None], dict[str, Any]],
    ) -> dict[str, Any]:
        with self._lock:
            identity = self._identity(key)
            current = self._records.get(identity)
            next_payload = transform(
                copy.deepcopy(current) if current is not None else None
            )
            self._records[identity] = copy.deepcopy(dict(next_payload))
            return copy.deepcopy(next_payload)

    def quarantine_storage(
        self,
        *,
        board_id: str,
        graph_type: str,
        affected_storage_refs: Sequence[StorageRef],
        reason: str,
        reason_bucket: str,
        correlation_ids: Sequence[str],
        kg_generation_id: str | None,
        retention_days: int,
        scope_storage_refs: Sequence[StorageRef],
        base_storage_ref_hint: StorageRef | None = None,
    ) -> Mapping[str, Any]:
        from okto_pulse.core.kg.quarantine import (
            MANIFEST_FILENAME,
            QUARANTINE_DIRNAME,
            QuarantineError,
            QuarantineErrorCode,
        )

    def reference(self, key: RebuildAuditKey) -> str:
        return key.to_ref()

    def read_json_reference(self, reference: str) -> dict[str, Any] | None:
        with self._lock:
            for raw_key, payload in self._records.items():
                key = RebuildAuditKey(
                    namespace=raw_key[0],
                    board_id=raw_key[1],
                    kg_generation_id=raw_key[2],
                    artifact_id=raw_key[3],
                )
                if key.to_ref() == reference:
                    return copy.deepcopy(payload)
        return None

        roots = [_testing_path_from_ref(ref) for ref in scope_storage_refs]
        resolved_paths = [_testing_path_from_ref(ref) for ref in affected_storage_refs]
        for path in resolved_paths:
            if not any(_is_relative_to(path, root) for root in roots):
                raise QuarantineError(
                    QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE,
                    retryable=False,
                    reason=f"path {path} is not under any KG storage root",
                )

        quarantine_id = f"q_{uuid.uuid4().hex}"
        quarantine_dir = (
            self._quarantine_base(base_storage_ref_hint, roots)
            / QUARANTINE_DIRNAME
            / quarantine_id
        )
        try:
            quarantine_dir.mkdir(parents=True, exist_ok=False)
        except OSError as exc:
            raise QuarantineError(
                QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
                retryable=True,
                reason=f"mkdir failed: {exc}",
            ) from exc

        moved_relatives: list[str] = []
        files_moved = 0
        try:
            for source in resolved_paths:
                moved_relatives.append(source.name)
                if source.exists():
                    shutil.move(str(source), str(quarantine_dir / source.name))
                    files_moved += 1

            now = datetime.now(timezone.utc)
            manifest = {
                "quarantine_id": quarantine_id,
                "board_id": board_id,
                "graph_type": graph_type,
                "reason": reason,
                "reason_bucket": reason_bucket,
                "correlation_ids": list(correlation_ids),
                "affected_paths_relative": moved_relatives,
                "kg_generation_id": kg_generation_id,
                "software_version": "test",
                "quarantined_at": now.isoformat(),
                "retention_until": (now + timedelta(days=retention_days)).isoformat(),
                "files_moved": files_moved,
            }
            manifest_path = quarantine_dir / MANIFEST_FILENAME
            from okto_pulse.core.kg import quarantine as quarantine_module

            with manifest_path.open("w", encoding="utf-8") as fh:
                quarantine_module.json.dump(manifest, fh, indent=2)
        except Exception as exc:
            partial_dir = quarantine_dir.with_name(quarantine_dir.name + ".partial")
            try:
                if partial_dir.exists():
                    partial_dir = quarantine_dir.with_name(
                        f"{quarantine_dir.name}.partial.{uuid.uuid4().hex[:8]}"
                    )
                quarantine_dir.rename(partial_dir)
            except OSError:
                pass
            raise QuarantineError(
                QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
                retryable=True,
                reason=f"manifest write failed: {exc} (preserved at {partial_dir})",
            ) from exc

        return {
            **manifest,
            "affected_storage_refs": [
                {"token": ref.token, "namespace": ref.namespace}
                for ref in affected_storage_refs
            ],
            "manifest_ref": str(manifest_path),
        }

    def list_quarantine_manifests(
        self,
        *,
        active_after_iso: str | None = None,
        base_storage_ref_hint: StorageRef | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        from okto_pulse.core.kg.quarantine import MANIFEST_FILENAME, QUARANTINE_DIRNAME

        root = self._quarantine_base(base_storage_ref_hint, ()) / QUARANTINE_DIRNAME
        if not root.exists():
            return []

        active_after = (
            datetime.fromisoformat(active_after_iso) if active_after_iso else None
        )
        rows: list[dict[str, Any]] = []
        for manifest_path in sorted(root.glob(f"*/{MANIFEST_FILENAME}")):
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if active_after is not None:
                retention_until = datetime.fromisoformat(
                    str(payload["retention_until"])
                )
                if retention_until <= active_after:
                    continue
            rows.append(copy.deepcopy(payload))
        return rows

    def read_quarantine_manifest(
        self,
        *,
        quarantine_id: str,
        base_storage_ref_hint: StorageRef | None = None,
    ) -> Mapping[str, Any] | None:
        from okto_pulse.core.kg.quarantine import MANIFEST_FILENAME, QUARANTINE_DIRNAME

        path = (
            self._quarantine_base(base_storage_ref_hint, ())
            / QUARANTINE_DIRNAME
            / quarantine_id
            / MANIFEST_FILENAME
        )
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _quarantine_base(
        base_storage_ref_hint: StorageRef | None,
        roots: Sequence[Path],
    ) -> Path:
        if base_storage_ref_hint is not None:
            return _testing_path_from_ref(base_storage_ref_hint)
        if roots:
            return Path(roots[0]).parent
        return Path.cwd()

    def reset_for_tests(self) -> None:
        with self._lock:
            self._records.clear()


class InMemoryCognitivePendingWorkProvider(CognitivePendingWorkProvider):
    """Test fake for pending-work discovery; default registry starts empty."""

    def __init__(self, records: Sequence[CognitivePendingRecordRef] = ()) -> None:
        self._records = tuple(records)

    def list_records(self) -> Sequence[CognitivePendingRecordRef]:
        return self._records


class InMemoryRebuildAuditArtifactStoreResolver(RebuildAuditArtifactStoreResolver):
    """Resolve opaque scopes to isolated in-memory stores for pure-core tests."""

    def __init__(self) -> None:
        self._stores: dict[str, InMemoryRebuildAuditArtifactStore] = {}
        self._lock = threading.Lock()

    def resolve(self, scope: object) -> RebuildAuditArtifactStore:
        identity = f"{type(scope).__module__}.{type(scope).__qualname__}:{scope!s}"
        with self._lock:
            return self._stores.setdefault(
                identity, InMemoryRebuildAuditArtifactStore()
            )


__all__ = [
    "InMemoryCognitivePendingWorkProvider",
    "InMemoryRebuildAuditArtifactStore",
    "InMemoryRebuildAuditArtifactStoreResolver",
]


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _testing_path_from_ref(ref: StorageRef) -> Path:
    if ref.namespace == "community_local_graph_v1":
        padding = "=" * (-len(ref.token) % 4)
        raw = base64.urlsafe_b64decode(ref.token + padding).decode("utf-8")
        return Path(raw).resolve()
    return Path(ref.token).resolve()
