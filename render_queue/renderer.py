"""Deterministic fixture-backed renderer used by worker processes."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple

FIXTURE_FILES = ("scene.json", "palette.json", "stamp.txt")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fixture_manifest(fixtures_dir: str | Path) -> Dict[str, Any]:
    root = Path(fixtures_dir)
    files: Dict[str, Dict[str, Any]] = {}
    aggregate = hashlib.sha256()
    for name in FIXTURE_FILES:
        path = root / name
        data = path.read_bytes()
        files[name] = {"sha256": sha256_bytes(data), "bytes": len(data)}
        aggregate.update(name.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(data)
        aggregate.update(b"\0")
    return {
        "fixtures_dir": str(root),
        "files": files,
        "manifest_sha256": aggregate.hexdigest(),
    }


def render_asset(
    fixtures_dir: str | Path,
    asset_id: str,
    asset_version: str,
    params: Mapping[str, Any],
    delay_seconds: float = 0.0,
) -> Tuple[bytes, str, str]:
    """Render a small deterministic artifact from the checked-in fixtures."""
    if delay_seconds > 0:
        time.sleep(delay_seconds)

    root = Path(fixtures_dir)
    scene = json.loads((root / "scene.json").read_text(encoding="utf-8"))
    palette = json.loads((root / "palette.json").read_text(encoding="utf-8"))
    stamp = (root / "stamp.txt").read_text(encoding="utf-8").strip()
    manifest = fixture_manifest(root)

    # The output depends only on the fixed fixtures, material version and
    # canonical parameters.  No wall clock, PID or owner is included.
    document = {
        "format": "rq-fixture-render/v1",
        "asset_id": asset_id,
        "asset_version": asset_version,
        "params": params,
        "canonical_params_sha256": sha256_bytes(canonical_json(params).encode("utf-8")),
        "scene": {
            "name": scene.get("name"),
            "objects": scene.get("objects", []),
            "resolution": scene.get("resolution"),
        },
        "palette": palette,
        "stamp": stamp,
        "fixture_sha256": manifest["manifest_sha256"],
    }
    body = b"RQRENDER-v1\n" + canonical_json(document).encode("utf-8") + b"\n"
    return body, sha256_bytes(body), "application/x-rq-fixture-render"
