"""Sphinx configuration for the gcamreader documentation."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version

project = "gcamreader"
copyright = "2024, Joint Global Change Research Institute"
author = "Joint Global Change Research Institute"

try:
    release = _pkg_version("gcamreader")
except Exception:  # pragma: no cover - fallback when not installed
    release = "1.5.0"
version = ".".join(release.split(".")[:2])

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

html_theme = "furo"
html_static_path = ["_static"]

autodoc_typehints = "description"
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
}
