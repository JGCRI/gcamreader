Changelog
=========

All notable changes to ``gcamreader`` are documented here. The format is based on
`Keep a Changelog <https://keepachangelog.com/en/1.1.0/>`_, and the project adheres
to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

This page is the canonical, rendered changelog. Each released version also records
its **release verification** — the behavioral regression results proving that the
release reproduces the previous version's query outputs exactly. New releases
should add their own ``Release verification`` block following the pattern shown for
:ref:`changelog-1-5-0` below.

.. _changelog-unreleased:

Unreleased
----------

No unreleased changes yet.

.. _changelog-1-5-0:

1.5.0 — 2026-06-15
------------------

This release modernizes the package without changing its computational results.
A behavioral baseline captured from v1.4.0 is preserved in ``benchmarks/`` and is
used as a regression gate to guarantee identical query outputs.

Added
~~~~~

- Behavioral baseline harness and regression test suite in ``benchmarks/`` that
  lock query, scenario, ``importdata``, and CLI outputs to the v1.4.0 results.
- ``pyproject.toml`` as the single source of build metadata and configuration.
- ``CHANGELOG.md`` to track project changes.

Changed
~~~~~~~

- Migrated packaging from ``setup.py`` and ``setup.cfg`` to ``pyproject.toml``
  (setuptools backend).
- Raised the minimum supported Python version to 3.11.
- Replaced the Click based command line interface (and the
  ``click-default-group-wheel`` dependency) with a Typer based CLI.
- Relaxed the pinned ``requests~=2.20.0`` requirement to a modern minimum and
  refreshed the ``pandas`` and ``lxml`` minimum versions.
- Modernized ``querymi.py``: removed Python 2 and Python 3.5 compatibility
  branches, replaced ``pkg_resources`` with ``importlib.resources``, and added
  type hints and complete docstrings.
- Moved the test suite to a top level ``tests/`` directory and the example
  notebooks to a top level ``notebooks/`` directory.

Fixed
~~~~~

- Result parsing in ``_parserslt`` now aggregates with ``dropna=False``, so rows
  that contain missing values in one or more grouping columns are retained
  instead of being silently dropped during the ``groupby().sum()`` step (PR #40).

Removed
~~~~~~~

- ``setup.py``, ``setup.cfg``, ``requirements.txt``, and ``MANIFEST.in``
  (superseded by ``pyproject.toml``).

.. _changelog-1-5-0-verification:

Release verification
~~~~~~~~~~~~~~~~~~~~~~

**Result: PASS — v1.5.0 reproduces the v1.4.0 outputs exactly.**

Because v1.5.0 is a modernization release, its central guarantee is that it
changes *no* computational results. To prove this, a behavioral baseline was
captured from **v1.4.0** and committed under ``benchmarks/baseline/``; the
v1.5.0 code is then re-run through the same operations and its outputs are
compared against that baseline by ``benchmarks/test_regression.py``.

Every fixture matches the v1.4.0 baseline. CSV outputs are compared
byte-for-byte by SHA-256 checksum, and DataFrame outputs are compared with
:func:`pandas.testing.assert_frame_equal` after a deterministic sort.

============================================  =========  ==================================  ========
Operation                                     Result     Comparison method                   Java
============================================  =========  ==================================  ========
``parse_batch_query`` (query structure)       identical  exact title/region/query strings    not req.
``listScenariosInDB``                         identical  ``assert_frame_equal``               required
``runQuery`` (land-allocation query)          identical  ``assert_frame_equal`` (sorted)      required
``importdata`` (land-allocation query)        identical  ``assert_frame_equal`` (sorted)      required
``gcamreader local`` CLI CSV output           identical  SHA-256 (byte-for-byte)              required
============================================  =========  ==================================  ========

The committed v1.4.0 baseline fixtures and their SHA-256 checksums
(from ``benchmarks/baseline/manifest.json``) are:

==========================================  ==================================================================
Fixture                                     SHA-256
==========================================  ==================================================================
``cli_outputs/crop_land_allocation.csv``    ``82dd106d12e14f32d4e9828e5a4ebb3d0cb78580da33dc5de9a8b9f0124f250b``
``parse_batch_query.json``                  ``b83c9a39d7aee7cbc76535c4bb154bdafdfe96e550ac1709740bd856eb3fbdd2``
``query_outputs/importdata_land.csv``       ``a7574c25b04446bcb27029b4547acac45f546415ebd5cc3fa310bd8c9742a445``
``query_outputs/land_query.csv``            ``a7574c25b04446bcb27029b4547acac45f546415ebd5cc3fa310bd8c9742a445``
``query_outputs/list_scenarios.csv``        ``341660163c5f0c578f0aa44cdfe29dee4feca5af030bdeeea4652cccb98da690``
==========================================  ==================================================================

Baseline capture environment:

================  ====================================================
Component         Value
================  ====================================================
Baseline version  ``gcamreader`` 1.4.0
Python            ``3.13.3``
Java              OpenJDK ``23.0.2`` (Homebrew)
Platform          macOS 26.5.1 (arm64)
Benchmark query   Crop Land Allocation (bundled land query)
================  ====================================================

Reproduce the parity check locally with:

.. code-block:: bash

   pytest benchmarks/test_regression.py

The structural ``parse_batch_query`` check runs without Java; the
database-backed checks require a Java runtime and the bundled sample database.
See :doc:`verified_versions` for the separate study confirming compatibility
across GCAM database versions 5.3 through 9.1.
