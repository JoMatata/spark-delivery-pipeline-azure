import os

import pytest

from pipeline.config import REPO_ROOT, load_config


def test_local_profile_resolves_relative_paths():
    cfg = load_config(profile="local")
    assert cfg.is_local
    assert cfg.path("silver", "orders") == str(REPO_ROOT / "data" / "lake" / "silver") + "/orders"
    assert cfg.pipeline["bbox"]["min_lat"] == -1.45


def test_azure_profile_builds_abfss_paths(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "stjoanlake")
    cfg = load_config(profile="azure")
    assert not cfg.is_local
    assert cfg.path("gold", "daily_zone_kpis") == \
        "abfss://lakehouse@stjoanlake.dfs.core.windows.net/gold/daily_zone_kpis"
    assert cfg.spark["master"] is None


def test_azure_profile_default_account(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    assert "stnbodeliverylake" in load_config(profile="azure").path("bronze")


def test_profile_from_env_and_unknown_profile(monkeypatch):
    monkeypatch.setenv("PIPELINE_PROFILE", "azure")
    assert load_config().profile == "azure"
    with pytest.raises(ValueError):
        load_config(profile="gcp")


def test_storage_overrides(tmp_path):
    cfg = load_config(profile="local", overrides={"gold": str(tmp_path)})
    assert cfg.path("gold") == str(tmp_path)
    assert os.path.isabs(cfg.path("bronze"))
