"""
Model helper tests: PR/issue classification, breaking-change detection, and
DatabaseHelper serialization round-trips.
"""

from src.models import DatabaseHelper as H


def test_classify_pr_type_from_labels():
    assert H.classify_pr_type("anything", ["bug"]) == "bugfix"
    assert H.classify_pr_type("anything", ["enhancement"]) == "feature"
    assert H.classify_pr_type("anything", ["documentation"]) == "docs"
    assert H.classify_pr_type("anything", ["refactor"]) == "maintenance"


def test_classify_pr_type_from_title():
    assert H.classify_pr_type("Fix the crash", []) == "bugfix"
    assert H.classify_pr_type("Add a new plugin", []) == "feature"
    assert H.classify_pr_type("Update the README docs", []) == "docs"


def test_classify_issue_type_from_labels():
    assert H.classify_issue_type("anything", ["bug"]) == "bug"
    assert H.classify_issue_type("anything", ["enhancement"]) == "feature"
    assert H.classify_issue_type("anything", ["question"]) == "question"
    assert H.classify_issue_type("anything", ["documentation"]) == "documentation"


def test_classify_issue_type_from_title():
    assert H.classify_issue_type("There is a bug here", []) == "bug"
    assert H.classify_issue_type("How do I do this?", []) == "question"


def test_is_breaking_change_label():
    assert H.is_breaking_change("t", "", ["breaking-change"]) is True
    assert H.is_breaking_change("t", "", ["major"]) is True


def test_is_breaking_change_text():
    assert H.is_breaking_change("t", "this is a breaking change", []) is True
    assert H.is_breaking_change("Backwards incompatible", "", []) is True


def test_is_breaking_change_false():
    assert H.is_breaking_change("Add a feature", "nothing special", []) is False


def test_labels_round_trip():
    labels = ["bug", "help wanted"]
    assert H.deserialize_labels(H.serialize_labels(labels)) == labels


def test_labels_empty_and_invalid():
    assert H.serialize_labels([]) == "[]"
    assert H.deserialize_labels("") == []
    assert H.deserialize_labels("not json") == []


def test_assignees_round_trip():
    assignees = ["alice", "bob"]
    assert H.deserialize_assignees(H.serialize_assignees(assignees)) == assignees


def test_extract_issue_numbers():
    assert H.extract_issue_numbers_from_text("fixes #12 and closes #34") == [12, 34]
    assert H.extract_issue_numbers_from_text("") == []


def test_time_to_merge_and_close():
    assert H.calculate_time_to_merge(
        "2024-01-01T00:00:00", "2024-01-02T00:00:00") == 24.0
    assert H.calculate_time_to_merge("2024-01-01T00:00:00", None) is None
    assert H.calculate_time_to_close(
        "2024-01-01T00:00:00", "2024-01-01T12:00:00") == 12.0
