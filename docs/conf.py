"""Sphinx configuration for the gcamreader documentation."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version
from typing import Any

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

# Hide the "gcamreader X.Y.Z documentation" text in the sidebar and show the logo
# instead. Furo supports separate logos for light and dark mode; the white logo is
# used for dark mode and the transparent (colored) logo for light mode so it stays
# visible against either sidebar background.
html_theme_options = {
    "sidebar_hide_name": True,
    "light_logo": "gcamreader-logo-transparent.png",
    "dark_logo": "gcamreader-logo-white.png",
}

autodoc_typehints = "description"
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
}


def _expand_furo_navigation_with_subsections(
    app: Any, pagename: str, templatename: str, context: dict[str, Any], doctree: Any
) -> None:
    """Render every page's subsections in the Furo sidebar navigation tree.

    By default Furo builds its sidebar ``furo_navigation_tree`` by calling
    Sphinx's global ``toctree`` with ``titles_only=True``, so only top-level
    page titles are shown. This handler rebuilds the navigation tree with
    ``titles_only=False`` so the in-page section headings for *all* pages are
    included at once. Furo's own post-processing (``get_navigation_tree``) is
    reused so the result keeps the native collapsible toggle arrows and styling.
    """
    if "toctree" not in context:
        return

    try:
        from furo.navigation import get_navigation_tree
    except Exception:  # pragma: no cover - Furo not installed / API changed
        return

    toctree = context["toctree"]
    toctree_html = toctree(
        collapse=False,
        titles_only=False,
        maxdepth=-1,
        includehidden=True,
    )
    context["furo_navigation_tree"] = get_navigation_tree(toctree_html)


def setup(app: Any) -> dict[str, Any]:
    # Run after Furo's own ``html-page-context`` handler (priority 500) so we can
    # overwrite the navigation tree it computed.
    app.connect(
        "html-page-context",
        _expand_furo_navigation_with_subsections,
        priority=900,
    )
    return {"parallel_read_safe": True, "parallel_write_safe": True}
