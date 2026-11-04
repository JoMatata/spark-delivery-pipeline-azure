"""Config loading: YAML profiles + ${VAR} / ${VAR:-default} environment expansion."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "config.yaml"
_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is None:
                raise KeyError(f"Environment variable {name} is required by the config")
            return default
        return _ENV.sub(repl, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def _is_uri(path: str) -> bool:
    return "://" in path


@dataclass
class PipelineConfig:
    profile: str
    storage: dict[str, str]
    spark: dict[str, Any]
    pipeline: dict[str, Any] = field(default_factory=dict)

    def path(self, layer: str, *parts: str) -> str:
        """Build a path inside a storage layer, e.g. cfg.path('silver', 'orders')."""
        base = self.storage[layer].rstrip("/")
        return "/".join([base, *parts]) if parts else base

    @property
    def is_local(self) -> bool:
        return not any(_is_uri(p) for p in self.storage.values())


def load_config(path: str | os.PathLike | None = None, profile: str | None = None,
                overrides: dict[str, str] | None = None) -> PipelineConfig:
    path = Path(path or os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG))
    raw = yaml.safe_load(path.read_text())
    profile = profile or os.environ.get("PIPELINE_PROFILE") or raw.get("default_profile", "local")
    if profile not in raw["profiles"]:
        raise ValueError(f"Unknown profile '{profile}'. Available: {sorted(raw['profiles'])}")
    prof = _expand(raw["profiles"][profile])

    storage = dict(prof["storage"])
    storage.update(overrides or {})
    # Relative local paths are resolved against the repo root so jobs can run from any cwd.
    for k, v in storage.items():
        if not _is_uri(v) and not os.path.isabs(v):
            storage[k] = str((REPO_ROOT / v).resolve())
    return PipelineConfig(profile=profile, storage=storage, spark=prof.get("spark", {}),
                          pipeline=_expand(raw.get("pipeline", {})))
