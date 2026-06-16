"""Unit tests for the :class:`gcamreader.Query` container and parsing helpers.

These tests exercise the pure-Python parsing logic of ``gcamreader`` and do not
require a Java runtime or a database connection.

@license BSD 2-Clause
"""

from __future__ import annotations

from pathlib import Path

import lxml.etree as ET
import pytest

import gcamreader
from gcamreader import querymi


class TestQuery:
    """Tests for constructing :class:`gcamreader.Query` objects."""

    def test_from_string_parses_title(self) -> None:
        """A query built from a string should expose its title."""
        xml = '<aQuery title="Population"><region name="China"/></aQuery>'
        query = gcamreader.Query(xml)
        assert query.title == "Population"

    def test_from_string_parses_single_region(self) -> None:
        """A query with one region should yield a one-element region list."""
        xml = '<aQuery title="Population"><region name="China"/></aQuery>'
        query = gcamreader.Query(xml)
        assert query.regions == ["China"]

    def test_from_string_parses_multiple_regions(self) -> None:
        """Region order should be preserved when several are present."""
        xml = (
            '<aQuery title="Emissions">'
            '<region name="USA"/>'
            '<region name="China"/>'
            '<region name="India"/>'
            "</aQuery>"
        )
        query = gcamreader.Query(xml)
        assert query.regions == ["USA", "China", "India"]

    def test_no_regions_yields_none(self) -> None:
        """A query without regions should have ``regions`` set to ``None``."""
        xml = '<aQuery title="Global"></aQuery>'
        query = gcamreader.Query(xml)
        assert query.regions is None

    def test_missing_title_yields_none(self) -> None:
        """A query without a title attribute should have ``title`` of ``None``."""
        xml = '<aQuery><region name="USA"/></aQuery>'
        query = gcamreader.Query(xml)
        assert query.title is None

    def test_querystr_is_roundtrippable_xml(self) -> None:
        """``querystr`` should be a serialized form parseable back into XML."""
        xml = '<aQuery title="Population"><region name="China"/></aQuery>'
        query = gcamreader.Query(xml)
        assert isinstance(query.querystr, str)
        reparsed = ET.XML(query.querystr)
        assert reparsed.get("title") == "Population"

    def test_from_element_matches_string(self) -> None:
        """Building from an lxml element should match building from a string."""
        xml = '<aQuery title="Population"><region name="China"/></aQuery>'
        element = ET.XML(xml)
        from_element = gcamreader.Query(element)
        from_string = gcamreader.Query(xml)
        assert from_element.title == from_string.title
        assert from_element.regions == from_string.regions

    def test_cdata_is_preserved(self) -> None:
        """CDATA sections in the query should survive serialization."""
        xml = (
            '<aQuery title="WithCData">'
            "<query><![CDATA[some & xpath < expression]]></query>"
            "</aQuery>"
        )
        query = gcamreader.Query(xml)
        assert "some & xpath < expression" in query.querystr


class TestParseBatchQuery:
    """Tests for :func:`gcamreader.parse_batch_query`."""

    def test_returns_query_objects(self, land_query_path: Path) -> None:
        """The parser should return a list of :class:`Query` objects."""
        queries = gcamreader.parse_batch_query(str(land_query_path))
        assert isinstance(queries, list)
        assert all(isinstance(q, gcamreader.Query) for q in queries)

    def test_bundled_query_title(self, land_query_path: Path) -> None:
        """The bundled land query should parse with the expected title."""
        queries = gcamreader.parse_batch_query(str(land_query_path))
        assert len(queries) == 1
        assert queries[0].title == "Crop Land Allocation"

    def test_parses_multiple_titles(self, tmp_path: Path) -> None:
        """Every element with a title attribute should produce a query."""
        xml = (
            "<queries>"
            '<aQuery title="First"><region name="USA"/></aQuery>'
            '<aQuery title="Second"><region name="China"/></aQuery>'
            "</queries>"
        )
        query_file = tmp_path / "queries.xml"
        query_file.write_text(xml)
        queries = gcamreader.parse_batch_query(str(query_file))
        titles = sorted(q.title for q in queries)
        assert titles == ["First", "Second"]

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        """Parsing a non-existent file should raise an error."""
        with pytest.raises((OSError, ET.XMLSyntaxError)):
            gcamreader.parse_batch_query(str(tmp_path / "does_not_exist.xml"))


class TestQuerylist:
    """Tests for the internal :func:`querymi._querylist` formatter."""

    def test_none_returns_empty_sequence(self) -> None:
        """``None`` should map to an empty XQuery sequence."""
        assert querymi._querylist(None) == "()"

    def test_empty_list_returns_empty_sequence(self) -> None:
        """An empty list should map to an empty XQuery sequence."""
        assert querymi._querylist([]) == "()"

    def test_single_string_is_wrapped(self) -> None:
        """A bare string should be treated as a single-element list."""
        assert querymi._querylist("USA") == "('USA')"

    def test_list_is_quoted_and_joined(self) -> None:
        """A multi-element list should be quoted and comma-joined."""
        assert querymi._querylist(["USA", "China"]) == "('USA','China')"


class TestDataDirs:
    """Tests for the package data directory helpers."""

    def test_sample_data_dir_exists(self) -> None:
        """The sample data directory should exist on disk."""
        assert Path(gcamreader.sample_data_dir()).is_dir()

    def test_sample_data_dir_contains_queries(self) -> None:
        """The sample data directory should contain the queries folder."""
        assert (Path(gcamreader.sample_data_dir()) / "queries").is_dir()

    def test_modelinterface_dir_exists(self) -> None:
        """The bundled ModelInterface directory should exist on disk."""
        assert Path(querymi._modelinterface_dir()).is_dir()

    def test_default_classpath_references_jar(self) -> None:
        """The default class path should reference ``ModelInterface.jar``."""
        assert "ModelInterface.jar" in querymi._default_miclasspath
