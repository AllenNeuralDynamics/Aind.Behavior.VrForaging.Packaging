"""Unit tests for PipelineConfig — YAML loading, defaults, extra='forbid' (§13)."""

import pytest
from aind_behavior_vr_foraging_server.config import PipelineConfig
from pydantic import ValidationError


def _write(tmp_path, text: str):
    p = tmp_path / "config.yaml"
    p.write_text(text)
    return p


class TestFromYaml:
    def test_minimal_config_loads_with_defaults(self, tmp_path):
        path = _write(tmp_path, "release: rel1\noutput:\n  uri: s3://bucket/prefix/\n")
        cfg = PipelineConfig.from_yaml(path)
        assert cfg.release == "rel1"
        assert cfg.input.store == "mount"
        assert cfg.output.store == "s3"
        assert cfg.worker.max_concurrent_jobs == 3

    def test_default_staging_rules_present(self, tmp_path):
        path = _write(tmp_path, "release: rel1\noutput:\n  uri: s3://bucket/prefix/\n")
        cfg = PipelineConfig.from_yaml(path)
        paths = {r.path for r in cfg.staging.rules}
        assert paths == {"", "behavior", "behavior-videos", "original_metadata"}

    def test_unknown_top_level_key_rejected(self, tmp_path):
        path = _write(tmp_path, "release: rel1\noutput:\n  uri: s3://bucket/\ntypo_field: 1\n")
        with pytest.raises(ValidationError):
            PipelineConfig.from_yaml(path)

    def test_unknown_nested_key_rejected(self, tmp_path):
        path = _write(tmp_path, "release: rel1\noutput:\n  uri: s3://bucket/\n  typo: 1\n")
        with pytest.raises(ValidationError):
            PipelineConfig.from_yaml(path)

    def test_missing_required_output_uri_rejected(self, tmp_path):
        path = _write(tmp_path, "release: rel1\noutput: {}\n")
        with pytest.raises(ValidationError):
            PipelineConfig.from_yaml(path)

    def test_non_mapping_yaml_rejected(self, tmp_path):
        path = _write(tmp_path, "- just\n- a\n- list\n")
        with pytest.raises(ValueError):
            PipelineConfig.from_yaml(path)

    def test_legacy_fallback_parsed(self, tmp_path):
        path = _write(
            tmp_path,
            "release: rel1\noutput:\n  uri: s3://bucket/\n"
            "ingestion:\n  legacy_fallback:\n    project_name: X\n    session_before: '2026-01-01'\n",
        )
        cfg = PipelineConfig.from_yaml(path)
        assert cfg.ingestion.legacy_fallback is not None
        assert cfg.ingestion.legacy_fallback.project_name == "X"

    def test_env_override_takes_priority_over_yaml(self, tmp_path, monkeypatch):
        path = _write(tmp_path, "release: rel1\noutput:\n  uri: s3://bucket/\nworker:\n  max_concurrent_jobs: 3\n")
        monkeypatch.setenv("VRF__WORKER__MAX_CONCURRENT_JOBS", "9")
        cfg = PipelineConfig.from_yaml(path)
        assert cfg.worker.max_concurrent_jobs == 9
