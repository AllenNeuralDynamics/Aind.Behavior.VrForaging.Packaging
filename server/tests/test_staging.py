"""Unit tests for the staging rule engine (§10) — pure metadata decisions, no I/O."""

from processing_server.config import StagingConfig, StagingRule
from processing_server.staging import (
    ObjectRef,
    build_manifest,
    missing_required,
    select,
    total_bytes,
    within_budget,
)

_DEFAULT_RULES = StagingConfig().rules


def _refs(*keys: str) -> list[ObjectRef]:
    return [ObjectRef(key=k, size=100) for k in keys]


class TestSelect:
    def test_root_json_included_non_recursive(self):
        refs = _refs("data_description.json", "subject.json")
        selected = select(refs, _DEFAULT_RULES)
        assert {r.key for r in selected} == {"data_description.json", "subject.json"}

    def test_root_json_nested_excluded_by_non_recursive_rule(self):
        """The root rule is non-recursive — a *.json two levels deep must not match it."""
        refs = _refs("some_dir/nested.json")
        selected = select(refs, _DEFAULT_RULES)
        assert selected == []

    def test_behavior_folder_included_recursively_except_video(self):
        refs = _refs(
            "behavior/SoftwareEvents/Block.json",
            "behavior/Logs/rig_output.json",
            "behavior/Video/video.mp4",
        )
        selected = {r.key for r in select(refs, _DEFAULT_RULES)}
        assert "behavior/SoftwareEvents/Block.json" in selected
        assert "behavior/Logs/rig_output.json" in selected
        assert "behavior/Video/video.mp4" not in selected

    def test_behavior_path_matches_case_insensitively(self):
        """Legacy sessions use `Behavior/` (capital B); the rule says `behavior` (§10)."""
        refs = _refs("Behavior/Logs/rig_output.json")
        selected = select(refs, _DEFAULT_RULES)
        assert len(selected) == 1

    def test_behavior_videos_keeps_csv_and_json_drops_everything_else(self):
        refs = _refs(
            "behavior-videos/FaceCamera/metadata.csv",
            "behavior-videos/FaceCamera/video.mp4",
        )
        selected = {r.key for r in select(refs, _DEFAULT_RULES)}
        assert "behavior-videos/FaceCamera/metadata.csv" in selected
        assert "behavior-videos/FaceCamera/video.mp4" not in selected

    def test_unmatched_path_is_narrow_allow_list_not_deny_list(self):
        """A folder covered by no rule is not staged at all — new file types are
        skipped by default, not swept in (§10)."""
        refs = _refs("some_unexpected_folder/file.bin")
        assert select(refs, _DEFAULT_RULES) == []

    def test_include_then_exclude_precedence(self):
        rule = StagingRule(path="x", include=["**/*"], exclude=["**/*.tmp"])
        refs = _refs("x/keep.json", "x/skip.tmp")
        selected = {r.key for r in select(refs, [rule])}
        assert selected == {"x/keep.json"}

    def test_first_matching_rule_wins(self):
        rules = [
            StagingRule(path="a", include=["*.json"]),
            StagingRule(path="a", include=["**/*"]),  # would also match, but comes second
        ]
        refs = _refs("a/only.csv")
        # First rule only allows *.json, so `only.csv` is not selected even though
        # the second (more permissive) rule would have matched it.
        assert select(refs, rules) == []


class TestMissingRequired:
    def test_reports_missing(self):
        selected = _refs("subject.json")
        assert missing_required(selected, ["data_description.json", "subject.json"]) == ["data_description.json"]

    def test_zero_byte_file_counts_as_missing(self):
        selected = [ObjectRef(key="data_description.json", size=0)]
        assert missing_required(selected, ["data_description.json"]) == ["data_description.json"]

    def test_nothing_missing(self):
        selected = _refs("data_description.json")
        assert missing_required(selected, ["data_description.json"]) == []


class TestBudget:
    def test_total_bytes(self):
        refs = [ObjectRef(key="a", size=10), ObjectRef(key="b", size=20)]
        assert total_bytes(refs) == 30

    def test_within_budget_true(self):
        refs = [ObjectRef(key="a", size=10)]
        assert within_budget(refs, max_session_bytes=100) is True

    def test_within_budget_false(self):
        refs = [ObjectRef(key="a", size=1000)]
        assert within_budget(refs, max_session_bytes=100) is False


class TestManifest:
    def test_build_manifest_reports_selection(self):
        config = StagingConfig()
        selected = _refs("data_description.json", "subject.json")
        manifest = build_manifest("s3", selected, config)
        assert manifest.available_files == 2
        assert manifest.available_bytes == 200
        assert manifest.store == "s3"
        assert manifest.truncated is False
