"""Rules deciding which XML attributes identify elements and which are metadata.

GCAM output identifies most elements by ``name`` and period-like attributes
(``year``, ``vintage``, ``ageGroup``). Those *identity* attributes become part
of an element's path; every other attribute is *metadata* that is compared for
presence and value. A few attributes, such as the scenario name and run date,
are excluded because they describe the run rather than the model structure.

Element paths in rules are slash-joined tag names without identities, rooted
at the document element: ``/scenario/world/region/sector``. Attribute paths
append ``/@attr``: ``/scenario/@date``.

Rules can be loaded from a flat TOML file whose keys mirror
:meth:`ComparisonRules.to_dict`::

    identity_attributes = ["name", "year", "vintage", "ageGroup"]
    excluded_attributes = ["/scenario/@name", "/scenario/@date"]
    ignored_attributes = ["fillout"]

    [identity_by_element]
    technology = ["name", "year"]
"""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields
from types import MappingProxyType
from typing import Any

from gcamreader.structure.errors import RulesError

#: Identity attributes used for every element unless overridden per tag.
DEFAULT_IDENTITY_ATTRIBUTES: tuple[str, ...] = ("name", "year", "vintage", "ageGroup")

#: Attribute paths excluded by default: they describe the run, not the model.
DEFAULT_EXCLUDED_ATTRIBUTES: tuple[str, ...] = ("/scenario/@name", "/scenario/@date")

_ATTR_NAME_RE = re.compile(r"^[^\s/@\[\]=\"]+$")
_TAG_NAME_RE = re.compile(r"^(?:\{[^{}\s]*\})?[^\s/@\[\]=\"{}]+$")
_ELEMENT_PATH_RE = re.compile(r"^(?:/(?:\{[^{}\s]*\})?[^\s/@\[\]=\"{}]+)+$")
_ATTRIBUTE_PATH_RE = re.compile(
    r"^((?:/(?:\{[^{}\s]*\})?[^\s/@\[\]=\"{}]+)+)/@([^\s/@\[\]=\"]+)$"
)

Pairs = tuple[tuple[str, str], ...]


def _check_names(values: Iterable[Any], key: str, pattern: re.Pattern[str]) -> None:
    for value in values:
        if not isinstance(value, str) or not pattern.match(value):
            raise RulesError(
                f"{key}: {value!r} is not a valid attribute or tag name "
                "(no whitespace, '/', '@', '[', ']', '=' or quotes)"
            )


def _as_str_tuple(value: Any, key: str) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise RulesError(f"{key}: expected a list of strings, got {value!r}")
    items = tuple(value)
    for item in items:
        if not isinstance(item, str) or not item:
            raise RulesError(f"{key}: expected a list of strings, got {item!r}")
    return items


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for item in items:
        seen.setdefault(item, None)
    return tuple(seen)


@dataclass(frozen=True)
class AttributeClassifier:
    """Precomputed lookup tables derived from :class:`ComparisonRules`.

    Built once per extraction so the per-element hot loop performs plain
    dictionary and set lookups.

    Attributes:
        default_identity: Identity attributes for tags without an override.
        identity_by_tag: Identity attributes per overridden tag.
        ignored: Attribute names dropped everywhere.
        excluded_by_path: Attribute names dropped at specific element paths.
        excluded_elements: Element paths whose subtrees are skipped entirely.
    """

    default_identity: tuple[str, ...]
    identity_by_tag: Mapping[str, tuple[str, ...]]
    ignored: frozenset[str]
    excluded_by_path: Mapping[str, frozenset[str]]
    excluded_elements: frozenset[str]

    def identity_for(self, tag: str) -> tuple[str, ...]:
        """Return the ordered identity attributes that apply to ``tag``."""
        return self.identity_by_tag.get(tag, self.default_identity)

    def split(
        self, tag_path: str, tag: str, attrib: Mapping[str, str]
    ) -> tuple[Pairs, Pairs]:
        """Partition an element's attributes into identity and metadata pairs.

        Args:
            tag_path: Slash-joined tag path of the element, e.g.
                ``/scenario/world/region``.
            tag: The element's tag (last segment of ``tag_path``).
            attrib: The element's attributes.

        Returns:
            ``(identity, metadata)``. Identity pairs follow the configured
            attribute order; metadata pairs are sorted by name so the result
            does not depend on attribute order in the document.
        """
        dropped = self.ignored
        excluded = self.excluded_by_path.get(tag_path)
        if excluded:
            dropped = dropped | excluded
        order = self.identity_for(tag)
        identity = tuple(
            (k, attrib[k]) for k in order if k in attrib and k not in dropped
        )
        ident_keys = set(order)
        metadata = tuple(
            sorted(
                (k, v)
                for k, v in attrib.items()
                if k not in ident_keys and k not in dropped
            )
        )
        return identity, metadata


@dataclass(frozen=True)
class ComparisonRules:
    """Configuration of identity, metadata and exclusions for a comparison.

    Instances are immutable and validated on construction; invalid settings
    raise :class:`~gcamreader.structure.errors.RulesError`.

    Attributes:
        identity_attributes: Attributes that identify an element wherever
            they appear, in the order used when formatting paths.
        identity_by_element: Per-tag replacement of ``identity_attributes``.
        excluded_attributes: Exact attribute paths (``/scenario/@date``) that
            are not compared. Identity attributes may only be excluded on the
            document element; deeper identities such as ``region/@name``
            cannot be excluded.
        excluded_elements: Exact element paths whose subtrees are ignored.
        ignored_attributes: Attribute names dropped everywhere (metadata only;
            an identity attribute cannot be ignored).
    """

    identity_attributes: tuple[str, ...] = DEFAULT_IDENTITY_ATTRIBUTES
    identity_by_element: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    excluded_attributes: tuple[str, ...] = DEFAULT_EXCLUDED_ATTRIBUTES
    excluded_elements: tuple[str, ...] = ()
    ignored_attributes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        set_ = object.__setattr__
        set_(
            self,
            "identity_attributes",
            _dedupe(_as_str_tuple(self.identity_attributes, "identity_attributes")),
        )
        set_(
            self,
            "excluded_attributes",
            _dedupe(_as_str_tuple(self.excluded_attributes, "excluded_attributes")),
        )
        set_(
            self,
            "excluded_elements",
            _dedupe(_as_str_tuple(self.excluded_elements, "excluded_elements")),
        )
        set_(
            self,
            "ignored_attributes",
            _dedupe(_as_str_tuple(self.ignored_attributes, "ignored_attributes")),
        )
        if not isinstance(self.identity_by_element, Mapping):
            raise RulesError(
                f"identity_by_element: expected a table of tag -> list of attributes, got {self.identity_by_element!r}"
            )
        by_element = {}
        for tag, attrs in self.identity_by_element.items():
            if not isinstance(tag, str) or not _TAG_NAME_RE.match(tag):
                raise RulesError(
                    f"identity_by_element: {tag!r} is not a valid tag name"
                )
            by_element[tag] = _dedupe(
                _as_str_tuple(attrs, f"identity_by_element.{tag}")
            )
        set_(
            self,
            "identity_by_element",
            MappingProxyType(dict(sorted(by_element.items()))),
        )
        self._validate()

    def _validate(self) -> None:
        _check_names(self.identity_attributes, "identity_attributes", _ATTR_NAME_RE)
        _check_names(self.ignored_attributes, "ignored_attributes", _ATTR_NAME_RE)
        for tag, attrs in self.identity_by_element.items():
            _check_names(attrs, f"identity_by_element.{tag}", _ATTR_NAME_RE)
        if not self.identity_attributes and not self.identity_by_element:
            raise RulesError(
                "identity_attributes: at least one identity attribute is required"
            )

        ignored = set(self.ignored_attributes)
        clash = ignored & set(self.identity_attributes)
        for attrs in self.identity_by_element.values():
            clash |= ignored & set(attrs)
        if clash:
            raise RulesError(
                "ignored_attributes: "
                + ", ".join(sorted(clash))
                + " are identity attributes and cannot be ignored"
            )

        for path in self.excluded_elements:
            if not _ELEMENT_PATH_RE.match(path):
                raise RulesError(
                    f"excluded_elements: {path!r} must be an absolute tag path such as /scenario/world/region/sector"
                )
            if path.count("/") == 1:
                raise RulesError(
                    f"excluded_elements: {path!r} is the document element and cannot be excluded"
                )

        for path in self.excluded_attributes:
            match = _ATTRIBUTE_PATH_RE.match(path)
            if match is None:
                raise RulesError(
                    f"excluded_attributes: {path!r} must be an absolute attribute path such as /scenario/@date"
                )
            element_path, attr = match.groups()
            tag = element_path.rsplit("/", 1)[1]
            is_root = element_path.count("/") == 1
            if not is_root and attr in self.identity_by_element.get(
                tag, self.identity_attributes
            ):
                raise RulesError(
                    f"excluded_attributes: {path!r} is an identity attribute below the document element and cannot be excluded"
                )

    # -- construction ------------------------------------------------------

    @classmethod
    def default(cls) -> ComparisonRules:
        """Return the default rules for GCAM output databases."""
        return cls()

    @classmethod
    def from_mapping(
        cls, mapping: Mapping[str, Any], *, source: str = "rules"
    ) -> ComparisonRules:
        """Build rules from a dictionary shaped like :meth:`to_dict`.

        Args:
            mapping: Keys are the field names of :class:`ComparisonRules`.
                Missing keys keep their defaults.
            source: Label used in error messages, typically a file name.

        Raises:
            RulesError: On unknown keys or invalid values.
        """
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(mapping) - known)
        if unknown:
            raise RulesError(
                f"{source}: unknown key(s) {', '.join(unknown)}; expected one of {', '.join(sorted(known))}"
            )
        try:
            return cls(**dict(mapping))
        except RulesError as exc:
            raise RulesError(f"{source}: {exc}") from None

    @classmethod
    def from_toml(cls, path: str) -> ComparisonRules:
        """Load rules from a TOML file.

        Raises:
            RulesError: If the file cannot be read or parsed, or its content
                is invalid.
        """
        try:
            with open(path, "rb") as handle:
                data = tomllib.load(handle)
        except OSError as exc:
            raise RulesError(f"{path}: cannot read rules file: {exc}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise RulesError(f"{path}: invalid TOML: {exc}") from exc
        return cls.from_mapping(data, source=str(path))

    def with_overrides(
        self,
        *,
        identity_attributes: Iterable[str] | None = None,
        ignored_attributes: Iterable[str] | None = None,
    ) -> ComparisonRules:
        """Return a copy with command-line overrides applied.

        Args:
            identity_attributes: Replaces the global identity attributes when
                given (per-element overrides are kept).
            ignored_attributes: Appended to the ignored attributes.
        """
        identity = (
            self.identity_attributes
            if identity_attributes is None
            else tuple(identity_attributes)
        )
        ignored = self.ignored_attributes
        if ignored_attributes is not None:
            ignored = ignored + tuple(ignored_attributes)
        return ComparisonRules(
            identity_attributes=identity,
            identity_by_element=dict(self.identity_by_element),
            excluded_attributes=self.excluded_attributes,
            excluded_elements=self.excluded_elements,
            ignored_attributes=ignored,
        )

    # -- serialization -----------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dictionary; the inverse of :meth:`from_mapping`."""
        return {
            "identity_attributes": list(self.identity_attributes),
            "identity_by_element": {
                tag: list(attrs) for tag, attrs in self.identity_by_element.items()
            },
            "excluded_attributes": list(self.excluded_attributes),
            "excluded_elements": list(self.excluded_elements),
            "ignored_attributes": list(self.ignored_attributes),
        }

    def fingerprint(self) -> str:
        """Return a stable hex digest of the rules, used to key index reuse."""
        canonical = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def describe(self) -> list[str]:
        """Return human-readable lines describing the active rules.

        The last line always discloses the fallback: attributes not listed as
        identity are compared as metadata, unfamiliar elements are traversed
        like any other, and element text (values) is never compared.
        """
        lines = ["Identity attributes: " + ", ".join(self.identity_attributes)]
        for tag, attrs in self.identity_by_element.items():
            lines.append(f"Identity attributes for <{tag}>: " + ", ".join(attrs))
        lines.append(
            "Excluded attributes: " + (", ".join(self.excluded_attributes) or "none")
        )
        lines.append(
            "Excluded elements: " + (", ".join(self.excluded_elements) or "none")
        )
        lines.append(
            "Ignored attributes: " + (", ".join(self.ignored_attributes) or "none")
        )
        lines.append(
            "Fallback: every other attribute is metadata compared for presence and "
            "value; unfamiliar elements are traversed like any other; element text "
            "(numeric values) is never compared."
        )
        return lines

    # -- classification ----------------------------------------------------

    def classifier(self) -> AttributeClassifier:
        """Return precomputed lookup tables for the extraction hot loop."""
        excluded_by_path: dict[str, set[str]] = {}
        for path in self.excluded_attributes:
            match = _ATTRIBUTE_PATH_RE.match(path)
            assert match is not None  # validated in __post_init__
            element_path, attr = match.groups()
            excluded_by_path.setdefault(element_path, set()).add(attr)
        return AttributeClassifier(
            default_identity=self.identity_attributes,
            identity_by_tag=self.identity_by_element,
            ignored=frozenset(self.ignored_attributes),
            excluded_by_path=MappingProxyType(
                {path: frozenset(attrs) for path, attrs in excluded_by_path.items()}
            ),
            excluded_elements=frozenset(self.excluded_elements),
        )

    def is_excluded_element(self, tag_path: str) -> bool:
        """Whether the element at ``tag_path`` (and its subtree) is ignored."""
        return tag_path in self.excluded_elements

    def split_attributes(
        self, tag_path: str, attrib: Mapping[str, str]
    ) -> tuple[Pairs, Pairs]:
        """Partition attributes into identity and metadata pairs.

        Convenience wrapper around :meth:`AttributeClassifier.split`; the
        extractor builds one classifier and calls it directly.

        Args:
            tag_path: Absolute tag path such as ``/scenario/world/region``.
            attrib: The element's attributes.
        """
        tag = tag_path.rsplit("/", 1)[1]
        return self.classifier().split(tag_path, tag, attrib)
