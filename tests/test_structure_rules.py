"""Unit tests for :mod:`gcamreader.structure.rules`.

@license BSD 2-Clause
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gcamreader.structure.errors import RulesError
from gcamreader.structure.rules import (
    DEFAULT_EXCLUDED_ATTRIBUTES,
    DEFAULT_IDENTITY_ATTRIBUTES,
    ComparisonRules,
)

REGION = "/scenario/world/region"
TECH = "/scenario/world/region/sector/subsector/technology"


def test_defaults() -> None:
    rules = ComparisonRules.default()
    assert rules.identity_attributes == ("name", "year", "vintage", "ageGroup")
    assert rules.identity_attributes == DEFAULT_IDENTITY_ATTRIBUTES
    assert rules.excluded_attributes == DEFAULT_EXCLUDED_ATTRIBUTES
    assert rules.excluded_elements == ()
    assert rules.ignored_attributes == ()
    assert dict(rules.identity_by_element) == {}


def test_split_default_identity_in_configured_order() -> None:
    rules = ComparisonRules.default()
    # Document order is year, unit, name; identity must come out name, year.
    attrib = {"year": "2020", "unit": "EJ", "name": "coal", "type": "x"}
    identity, metadata = rules.split_attributes(TECH, attrib)
    assert identity == (("name", "coal"), ("year", "2020"))
    assert metadata == (("type", "x"), ("unit", "EJ"))  # sorted by name


def test_split_per_element_override_replaces_global_identity() -> None:
    rules = ComparisonRules(identity_by_element={"technology": ["year"]})
    identity, metadata = rules.split_attributes(
        TECH, {"name": "coal", "year": "2020", "unit": "EJ"}
    )
    assert identity == (("year", "2020"),)
    assert metadata == (("name", "coal"), ("unit", "EJ"))
    # Other tags keep the global identity.
    identity, _ = rules.split_attributes(REGION, {"name": "USA"})
    assert identity == (("name", "USA"),)


def test_split_excluded_attribute_only_at_exact_path() -> None:
    rules = ComparisonRules.default()
    identity, metadata = rules.split_attributes(
        "/scenario", {"name": "GCAM", "date": "2026-1-6", "other": "1"}
    )
    assert identity == ()
    assert metadata == (("other", "1"),)
    # The same attribute names deeper in the tree are untouched.
    identity, metadata = rules.split_attributes(REGION, {"name": "USA", "date": "d"})
    assert identity == (("name", "USA"),)
    assert metadata == (("date", "d"),)


def test_split_ignored_attribute_dropped_everywhere() -> None:
    rules = ComparisonRules(ignored_attributes=["fillout"])
    _, metadata = rules.split_attributes(TECH, {"fillout": "1", "unit": "EJ"})
    assert metadata == (("unit", "EJ"),)
    _, metadata = rules.split_attributes("/scenario", {"fillout": "1"})
    assert metadata == ()


def test_split_no_attributes() -> None:
    assert ComparisonRules.default().split_attributes("/scenario/world", {}) == ((), ())


def test_is_excluded_element() -> None:
    rules = ComparisonRules(excluded_elements=["/scenario/world/region/Marketplace"])
    assert rules.is_excluded_element("/scenario/world/region/Marketplace")
    assert not rules.is_excluded_element("/scenario/world/region")
    assert rules.classifier().excluded_elements == {
        "/scenario/world/region/Marketplace"
    }


def test_lists_are_normalized_and_deduplicated() -> None:
    rules = ComparisonRules(
        identity_attributes=["name", "name", "year"],
        ignored_attributes=("a", "a"),
    )
    assert rules.identity_attributes == ("name", "year")
    assert rules.ignored_attributes == ("a",)


def test_identity_by_element_is_read_only_and_sorted() -> None:
    rules = ComparisonRules(identity_by_element={"z": ["name"], "a": ["year"]})
    assert list(rules.identity_by_element) == ["a", "z"]
    with pytest.raises(TypeError):
        rules.identity_by_element["b"] = ("x",)  # type: ignore[index]


# --- validation -----------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "key", "fragment"),
    [
        ({"identity_attributes": "name"}, "identity_attributes", "list of strings"),
        ({"identity_attributes": ["na me"]}, "identity_attributes", "not a valid"),
        (
            {"identity_attributes": [], "identity_by_element": {}},
            "identity_attributes",
            "at least one",
        ),
        ({"ignored_attributes": ["name"]}, "ignored_attributes", "identity attributes"),
        (
            {
                "identity_by_element": {"technology": ["vintage"]},
                "ignored_attributes": ["vintage"],
            },
            "ignored_attributes",
            "identity attributes",
        ),
        ({"identity_by_element": ["technology"]}, "identity_by_element", "table"),
        (
            {"identity_by_element": {"bad tag": ["name"]}},
            "identity_by_element",
            "tag name",
        ),
        (
            {"identity_by_element": {"technology": "name"}},
            "identity_by_element.technology",
            "list of strings",
        ),
        ({"excluded_elements": ["region"]}, "excluded_elements", "absolute tag path"),
        ({"excluded_elements": ["/scenario"]}, "excluded_elements", "document element"),
        (
            {"excluded_attributes": ["name"]},
            "excluded_attributes",
            "absolute attribute path",
        ),
        (
            {"excluded_attributes": ["/scenario/world/region/@name"]},
            "excluded_attributes",
            "identity attribute",
        ),
        (
            {"excluded_attributes": ["/scenario/world/region/@year"]},
            "excluded_attributes",
            "identity attribute",
        ),
    ],
)
def test_invalid_rules(kwargs: dict, key: str, fragment: str) -> None:
    with pytest.raises(RulesError) as excinfo:
        ComparisonRules(**kwargs)
    message = str(excinfo.value)
    assert key in message
    assert fragment in message


def test_excluding_identity_attribute_on_root_is_allowed() -> None:
    rules = ComparisonRules(excluded_attributes=["/scenario/@name", "/scenario/@year"])
    assert "/scenario/@year" in rules.excluded_attributes


def test_excluding_non_identity_attribute_below_root_is_allowed() -> None:
    rules = ComparisonRules(excluded_attributes=["/scenario/world/region/sector/@type"])
    _, metadata = rules.split_attributes(
        "/scenario/world/region/sector", {"name": "elec", "type": "x", "unit": "u"}
    )
    assert metadata == (("unit", "u"),)


def test_excluding_overridden_identity_below_root_rejected() -> None:
    with pytest.raises(RulesError, match="identity attribute"):
        ComparisonRules(
            identity_by_element={"sector": ["type"]},
            excluded_attributes=["/scenario/world/region/sector/@type"],
        )


# --- mapping / TOML ---------------------------------------------------------


def test_from_mapping_round_trip() -> None:
    rules = ComparisonRules(
        identity_attributes=["name", "year"],
        identity_by_element={"technology": ["name", "year"]},
        excluded_attributes=["/scenario/@date"],
        excluded_elements=["/scenario/world/region/Marketplace"],
        ignored_attributes=["fillout"],
    )
    again = ComparisonRules.from_mapping(rules.to_dict())
    assert again == rules
    assert again.to_dict() == rules.to_dict()


def test_from_mapping_missing_keys_keep_defaults() -> None:
    rules = ComparisonRules.from_mapping({"ignored_attributes": ["fillout"]})
    assert rules.identity_attributes == DEFAULT_IDENTITY_ATTRIBUTES
    assert rules.ignored_attributes == ("fillout",)


def test_from_mapping_unknown_key() -> None:
    with pytest.raises(RulesError, match="my.toml: unknown key\\(s\\) identity_attrs"):
        ComparisonRules.from_mapping({"identity_attrs": ["name"]}, source="my.toml")


def test_from_mapping_prefixes_source_on_invalid_value() -> None:
    with pytest.raises(RulesError, match="^my.toml: ignored_attributes"):
        ComparisonRules.from_mapping({"ignored_attributes": ["name"]}, source="my.toml")


def test_from_toml(tmp_path: Path) -> None:
    toml = tmp_path / "rules.toml"
    toml.write_text(
        'identity_attributes = ["name", "year"]\n'
        'excluded_attributes = ["/scenario/@name", "/scenario/@date"]\n'
        'ignored_attributes = ["fillout"]\n'
        "\n"
        "[identity_by_element]\n"
        'technology = ["name", "year"]\n',
        encoding="utf-8",
    )
    rules = ComparisonRules.from_toml(str(toml))
    assert rules.identity_attributes == ("name", "year")
    assert dict(rules.identity_by_element) == {"technology": ("name", "year")}
    assert rules.ignored_attributes == ("fillout",)
    assert rules.excluded_elements == ()


def test_from_toml_errors(tmp_path: Path) -> None:
    with pytest.raises(RulesError, match="cannot read"):
        ComparisonRules.from_toml(str(tmp_path / "missing.toml"))
    bad = tmp_path / "bad.toml"
    bad.write_text("identity_attributes = [", encoding="utf-8")
    with pytest.raises(RulesError, match="invalid TOML"):
        ComparisonRules.from_toml(str(bad))
    unknown = tmp_path / "unknown.toml"
    unknown.write_text('identity = ["name"]\n', encoding="utf-8")
    with pytest.raises(RulesError, match="unknown.toml: unknown key"):
        ComparisonRules.from_toml(str(unknown))


# --- overrides, fingerprint, describe ----------------------------------------


def test_with_overrides() -> None:
    base = ComparisonRules(
        identity_by_element={"technology": ["name"]}, ignored_attributes=["a"]
    )
    rules = base.with_overrides(identity_attributes=["name"], ignored_attributes=["b"])
    assert rules.identity_attributes == ("name",)
    assert rules.ignored_attributes == ("a", "b")
    assert dict(rules.identity_by_element) == {"technology": ("name",)}
    assert base.with_overrides() == base
    with pytest.raises(RulesError):
        base.with_overrides(ignored_attributes=["name"])


def test_fingerprint_stable_and_sensitive() -> None:
    a = ComparisonRules.default()
    b = ComparisonRules.from_mapping(a.to_dict())
    assert a.fingerprint() == b.fingerprint()
    assert len(a.fingerprint()) == 64
    c = ComparisonRules(ignored_attributes=["fillout"])
    assert c.fingerprint() != a.fingerprint()
    # Order of identity attributes matters (it defines path formatting).
    d = ComparisonRules(identity_attributes=["year", "name"])
    e = ComparisonRules(identity_attributes=["name", "year"])
    assert d.fingerprint() != e.fingerprint()


def test_describe_discloses_fallback() -> None:
    lines = ComparisonRules(identity_by_element={"technology": ["year"]}).describe()
    assert lines[0] == "Identity attributes: name, year, vintage, ageGroup"
    assert "Identity attributes for <technology>: year" in lines
    assert any(
        line.startswith("Excluded attributes: /scenario/@name") for line in lines
    )
    assert "Excluded elements: none" in lines
    assert "element text" in lines[-1]
    assert "metadata" in lines[-1]


def test_classifier_tables() -> None:
    rules = ComparisonRules(
        identity_by_element={"technology": ["year"]},
        excluded_attributes=[
            "/scenario/@name",
            "/scenario/@date",
            "/scenario/world/@foo",
        ],
        ignored_attributes=["fillout"],
    )
    classifier = rules.classifier()
    assert classifier.identity_for("technology") == ("year",)
    assert classifier.identity_for("region") == DEFAULT_IDENTITY_ATTRIBUTES
    assert classifier.ignored == {"fillout"}
    assert dict(classifier.excluded_by_path) == {
        "/scenario": {"name", "date"},
        "/scenario/world": {"foo"},
    }
    assert classifier.split("/scenario/world", "world", {"foo": "1", "bar": "2"}) == (
        (),
        (("bar", "2"),),
    )
