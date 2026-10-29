Structural comparison of databases
==================================

``gcamreader diff`` and :func:`gcamreader.compare_databases` compare the
*structure* of two or more local GCAM output databases: which regions,
sectors, subsectors, technologies, inputs, outputs, years, vintages and other
elements exist, and which metadata attributes (such as ``unit`` or ``type``)
they carry. Numeric values are ignored. The result is a Markdown report, and
optionally a JSON document, that says where two configurations produced a
different model structure.

Typical uses: confirming that a configuration change (a new technology, a
removed region, an extra period) shows up in the output database; checking that
two runs meant to be structurally identical really are; and reviewing a new
GCAM version's output against the previous one.

Quick start
-----------

.. code-block:: bash

   # Which runs are in each database? Needed only when a database holds several.
   gcamreader scenarios output/database_basexdb_ref output/database_basexdb_policy

   # Compare; keep the indexes so a second comparison is fast.
   gcamreader diff output/database_basexdb_ref output/database_basexdb_policy \
       -o report.md --json result.json --work-dir ~/.cache/gcamreader-structure

   echo $?   # 0: identical structure, 1: differences found, 2: error

.. code-block:: python

   import gcamreader

   result = gcamreader.compare_databases(
       ["output/database_basexdb_ref", "output/database_basexdb_policy"],
       work_dir="~/.cache/gcamreader-structure",
   )
   print(result.has_differences)
   for finding in result.comparisons[0].findings:
       print(finding.kind.value, finding.path)
   open("report.md", "w", encoding="utf-8").write(gcamreader.render_markdown(result))

How it works
------------

1. **Discovery.** Each database is fingerprinted (BaseX metadata file plus the
   names and sizes of the storage files) and its scenario runs are listed with
   their element counts.
2. **Selection.** Exactly one run is chosen per database. A database with one
   run needs no selector; otherwise ``--scenario NAME@DATE`` is required. Runs
   are never merged and the latest run is never picked silently.
3. **Extraction.** BaseX streams the scenario document; ``gcamreader`` parses it
   incrementally and writes a *structure index*, an SQLite file with one row per
   element path and metadata variant, occurrence counts, subtree sizes and an
   order-insensitive subtree fingerprint. The database itself is opened
   read-only and never modified. The index is reused on later runs when it is
   kept in a ``--work-dir``.
4. **Comparison.** The indexes are walked top-down from the document element.
   Subtrees with identical fingerprints are skipped; differing positions are
   classified as added, removed, attribute changed, count changed or metadata
   variants changed.
5. **Report.** The findings of every candidate against the baseline are rendered
   as Markdown, with a matrix across inputs, grouped by region or other
   top-level branch.

Rules: identity, metadata, exclusions
-------------------------------------

An element is identified among its siblings by its tag and its *identity
attributes*, by default ``name``, ``year``, ``vintage`` and ``ageGroup``. Every
other attribute is *metadata* and is compared for presence and value. Rules can
be adjusted with a TOML file (``--rules rules.toml``) whose keys mirror
:meth:`gcamreader.ComparisonRules.to_dict`:

.. code-block:: toml

   identity_attributes = ["name", "year", "vintage", "ageGroup"]
   excluded_attributes = ["/scenario/@name", "/scenario/@date"]
   excluded_elements = []
   ignored_attributes = ["fillout"]

   [identity_by_element]
   technology = ["name", "year"]

* ``identity_attributes``: identity everywhere, in path order.
* ``identity_by_element``: per-tag replacement of the identity list.
* ``excluded_attributes``: exact attribute paths that are not compared. The
  defaults remove the run name and date, which describe the run rather than the
  model. An identity attribute can only be excluded on the document element;
  ``/scenario/world/region/@name`` is rejected.
* ``excluded_elements``: exact element paths whose subtrees are ignored
  (``/scenario/world/Marketplace``).
* ``ignored_attributes``: metadata attribute names dropped everywhere.

``--identity-attr NAME`` (repeatable) replaces the global identity list and
``--ignore-attr NAME`` (repeatable) appends to the ignored attributes, on top of
the file or the defaults. The rules in force are printed in every report,
together with the fallback: unfamiliar elements are traversed like any other,
unlisted attributes are metadata, and element text is never compared.

Paths
-----

Paths are written as segments joined by `` > ``, each segment being
``tag[identity, ...]`` with identity attributes in rule order::

   scenario > world > region[name=USA] > sector[name=electricity] >
     subsector[name=coal] > technology[name="coal (conv pul)", year=2020]

Values are quoted only when they contain ``]``, ``,``, ``=``, a double quote or
leading/trailing whitespace. Namespaced tags appear in Clark notation
(``{uri}local``). In the API every :class:`~gcamreader.Finding` also carries
``segments``, the same path as data, so programs never need to parse the
string.

Counting definitions
--------------------

Occurrence / count
   How many sibling elements share a path and the same metadata. Children of
   repeated siblings (two ``<sector name="x">`` under one region) are merged;
   their counts are totals across the repeats, and a warning is recorded.
Added / removed
   A path present in one input only. Nothing below it is listed separately. The
   finding carries the number of *descendants* and, for subtrees with at least
   50 descendants, a histogram of their tags.
Grouped finding
   Added or removed siblings with the same parent and tag are reported as one
   finding whose path ends in a summary such as
   ``Non-CO2[year=2040..2100 (13 values)]``; ``members`` is the number of
   elements and the detail lists every member identity. Nothing is dropped;
   grouping keeps reports proportional to the number of changed branches.
Attribute changed
   Same path on both sides, one metadata variant each, different attribute
   values (added, removed or changed attributes are listed individually).
Count changed
   Same path and metadata, different number of occurrences.
Metadata variants changed
   A path whose occurrences carry several metadata variants on at least one
   side. All variants are listed with their counts; nothing is collapsed.

Matrix
   One row per differing path (and attribute), one column per input. A cell
   shows the value on that input: ``present``/``absent``, an attribute value, a
   count or the metadata variants. A candidate cell equal to the baseline means
   that comparison found no difference at that exact path; a difference in an
   ancestor is its own row and is not propagated to descendants.

Report layout
-------------

The report has, in order: the overall result; an inputs table (database,
``name@date``, GCAM version, element count, fingerprint, baseline marker); the
settings; the counting definitions; a summary per comparison; the matrix;
findings grouped by top-level branch (``region[name=USA]``, ``Marketplace``,
...) with an aggregate table by kind and tag; warnings and limitations.

Comparisons with more than ``--inline-limit`` findings (default 2,000) keep
only the aggregate tables in the report; every finding and the complete matrix
are then written to ``<report>.appendix.md`` next to the report. Without
``-o`` the report and the appendix are written to standard output. Nothing is
truncated in either case. ``--json`` writes the full result as JSON (the same
content as :meth:`gcamreader.ComparisonResult.to_dict`).

The report body is byte-stable: two runs on unchanged inputs produce identical
files. ``--timestamp`` appends a ``Generated:`` line.

Two example reports, produced from small test fixtures:
:doc:`examples/report_with_differences` and :doc:`examples/report_no_differences`.

.. toctree::
   :hidden:

   examples/report_with_differences
   examples/report_no_differences

Exit codes
----------

``gcamreader diff`` exits with

* ``0`` when no structural differences were found,
* ``1`` when differences were found,
* ``2`` on any error (usage, Java, database, extraction).

In a shell script::

   gcamreader diff ref policy -o report.md
   case $? in
     0) echo "identical structure" ;;
     1) echo "differences, see report.md" ;;
     *) echo "comparison failed" >&2; exit 1 ;;
   esac

Work directory and disk use
---------------------------

Each input gets a structure index of roughly the size of the database
(3.5 GB for a 3.2 GB database with 48 million elements). While an index is
being built a scratch file of similar size exists beside it, so plan for about
2.5 times the size of the largest database in free space. Free space is checked
before each extraction.

Without ``--work-dir`` the indexes live in a temporary directory that is removed
when the command ends. With ``--work-dir`` they are kept and reused: an index is
reused when the database fingerprint, the scenario, the rules and the
``gcamreader`` extractor version all match. ``--no-cache`` forces
re-extraction. The fingerprint does not hash the multi-gigabyte table files, so
an in-place modification that keeps file sizes is not detected; use
``--no-cache`` or delete the work directory in that case.

Java runtime
------------

The comparison runs the bundled BaseX jar in a Java subprocess. The jars are
Java 8 class files, so any Java 8 or newer runtime can load them; the project
tests with Temurin 17, which is the verified version. Newer runtimes are
accepted with a warning.

The runtime is located in this order: the ``GCAMREADER_JAVA`` environment
variable (a ``java`` executable or a JDK/JRE home directory), ``JAVA_HOME``,
then ``java`` on ``PATH``. ``GCAMREADER_JAVA`` is the way to use a user-space
JRE on machines where Java cannot be installed system-wide; on HPC systems a
``module load java`` before running is usually enough. No Java runtime is
bundled with ``gcamreader``. Common failures (no runtime, a runtime older than
Java 8, out of heap memory, module-system restrictions, a database written by
a newer BaseX than the bundled 9.5.0) are reported as one-line messages with
the remedy. ``--java-max-memory`` sets the JVM heap limit; the default ``4g``
is an upper bound, streaming a 48-million-element scenario needs well under
1 GB.

Limitations
-----------

* Only structure and metadata are compared; numeric values are ignored. A
  present element with value ``0`` is still present.
* GCAM writes only non-zero results and runs may cover different periods, so a
  path missing from one database can reflect output filtering or run coverage
  rather than a configuration change. Comparing a reference run with a strong
  mitigation scenario, for example, reports hundreds of thousands of removed
  outputs, inputs and emissions for technologies that phase out.
* A renamed or moved element appears as a removal plus an addition.
* Identity values are compared as text: ``2020`` and ``2020.0`` differ.
* Children of repeated siblings are merged (see *Occurrence / count*).
* Identical subtrees are recognised by a 128-bit additive fingerprint; an
  accidental collision hiding a difference is negligible for GCAM output and
  adversarial input is out of scope.
* Descendant tag histograms are given only for added or removed subtrees with
  at least 50 descendants.
* Remote databases are not supported; inputs must be local BaseX directories.

Performance
-----------

Measured on the two development databases (GCAM 9.0 reference and SSP4-2.6
runs, 48.5 and 47.6 million elements, 3.2 and 3.1 GB) on a 20-core Windows
workstation with an SSD, Temurin 17, through the command line:

.. list-table::
   :header-rows: 1

   * - Measure
     - Target
     - Measured
   * - Extraction of one ~50 M element database
     - 15 min or less
     - 7.5 and 11.3 min
   * - Comparison of the two databases (429 k grouped findings)
     - 5 min or less
     - 2.3 min
   * - Full cold run (list, extract both, compare, report, JSON)
     -
     - 21.6 min
   * - Warm re-run with reused indexes
     - 5 min or less
     - 3.4 min
   * - Self-comparison with reused index
     -
     - 9 s
   * - Peak Python memory
     - 2 GB or less
     - 1.6 GB
   * - Peak Java memory
     - 1 GB or less
     - 0.66 GB
   * - Work directory after a run
     - 1.5x total input size or less
     - 1.1x (6.9 GB for 6.3 GB)
   * - Transient disk during one extraction
     - 2.5x that database or less
     - 2.1x

Extraction is single-threaded and bound by Python-side work per element
(about 8 microseconds including the SQLite insert); BaseX streams the document
at 60 to 80 MiB/s and is never the bottleneck.

Implementation notes
--------------------

* ``gcamreader.structure.extract`` streams BaseX output through
  ``lxml.iterparse`` with an explicit frame stack. An element's position hash is
  ``blake2b(parent_hash + tag + identity)`` and is computed only for elements
  that have children; leaves need none. Rows are emitted when their parent
  closes so counts and metadata variants are complete.
* ``gcamreader.structure.index`` stores rows in SQLite. Rows are appended to a
  scratch table in an attached file during the stream, then aggregated once
  with ``GROUP BY`` into the ``node`` table (primary key: parent hash, tag,
  identity, metadata). The scratch file is deleted afterwards. An index records
  ``complete = 1`` last; readers refuse incomplete or differently versioned
  indexes.
* The subtree fingerprint is four 32-bit lanes of a blake2b digest over the
  position and metadata, summed with the children's lanes modulo 2^32. It is
  independent of sibling order, whitespace, comments and element text, so the
  comparison can stop at identical subtrees.
* ``gcamreader.structure.compare`` walks both indexes depth-first with sorted
  children, so the finding order is deterministic. Descendant histograms for
  one-sided subtrees are computed after the walk in batched recursive queries.
* Golden-value tests pin the fingerprints; a deliberate change of the hashing
  must bump ``EXTRACTOR_VERSION`` so that cached indexes are rebuilt.
