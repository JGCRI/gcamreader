Command line interface
=======================

``gcamreader`` installs a ``gcamreader`` command for running the queries in a
queries file and saving each result to a CSV file.

Version
-------

.. code-block:: bash

   gcamreader --version

Local database queries
-----------------------

.. code-block:: bash

   gcamreader local \
       --database_path path/to/output/database_basexdb \
       --query_path Main_queries.xml \
       --output_path results/ \
       --force

Options:

* ``-d``, ``--database_path``: path to the database directory (parent of the
  ``*.basex`` directory).
* ``-q``, ``--query_path``: path to the queries XML file to run.
* ``-o``, ``--output_path``: directory in which the result CSV files are
  created.
* ``-f``, ``--force``: overwrite existing CSV files in the output path.

Remote database queries
------------------------

.. code-block:: bash

   gcamreader remote \
       --username user \
       --database_name database_basexdb \
       --query_path Main_queries.xml \
       --output_path results/ \
       --hostname localhost \
       --port 8984

Options:

* ``-u``, ``--username``: username for remote server authentication.
* ``-w``, ``--password``: password for remote server authentication (prompted
  when omitted).
* ``-n``, ``--hostname``: hostname of the remote server (default
  ``localhost``).
* ``-p``, ``--port``: port on the remote server (default ``8984``).
* ``-d``, ``--database_name``: name of the database to query.
* ``-q``, ``--query_path``: path to the queries XML file to run.
* ``-o``, ``--output_path``: directory in which the result CSV files are
  created.
* ``-f``, ``--force``: overwrite existing CSV files in the output path.

Use ``gcamreader --help``, ``gcamreader local --help``, or
``gcamreader remote --help`` for the full list of options.

Listing scenarios
-----------------

.. code-block:: bash

   gcamreader scenarios output/database_basexdb_ref output/database_basexdb_policy --counts

Prints one row per run: input number, database, scenario name, date, GCAM
version and, with ``--counts``, the number of elements (a full traversal,
a few seconds per gigabyte). Use the ``NAME@DATE`` shown here with
``diff --scenario`` when a database holds several runs. Exit code 0 on
success, 2 on any error.

Options:

* ``--counts``: also count the elements of each scenario.
* ``--java-max-memory``: JVM heap limit (default ``4g``).
* ``--debug``: show tracebacks instead of messages.

Comparing database structure
----------------------------

.. code-block:: bash

   gcamreader diff output/database_basexdb_ref output/database_basexdb_policy \
       -o report.md \
       --json result.json \
       --work-dir ~/.cache/gcamreader-structure

Compares the structure of two or more local output databases (regions,
sectors, technologies, inputs, years and other elements and their metadata
attributes) and writes a Markdown report. Numeric values are ignored. See
:doc:`structure_comparison` for the rules, counting definitions, report layout
and limitations.

Exit codes: ``0`` no structural differences, ``1`` differences found, ``2``
any error.

Options:

* ``DB DB...``: two or more database directories (each containing ``*.basex``
  files). The first is the baseline unless ``--baseline`` is given. Remote
  databases are not supported.
* ``-o``, ``--output``: write the report to this file. When lists exceed
  ``--inline-limit`` an appendix ``<stem>.appendix.md`` is written beside it.
  Default: standard output (report followed by the appendix).
* ``--baseline IDX|PATH``: the baseline database, by 1-based position or path
  (default ``1``).
* ``-s``, ``--scenario [IDX=|BASENAME=]SELECTOR``: the run to compare, as
  ``NAME`` or ``NAME@DATE``, optionally prefixed with an input number or the
  database directory name to target one input. Repeatable; a bare selector
  applies to every database. Needed only when a database holds several runs.
* ``--rules FILE.toml``: comparison rules (identity, excluded and ignored
  attributes).
* ``--identity-attr NAME``: attribute treated as identity (repeatable; replaces
  the default list ``name``, ``year``, ``vintage``, ``ageGroup``).
* ``--ignore-attr NAME``: metadata attribute ignored everywhere (repeatable).
* ``--work-dir PATH``: directory for the structure indexes (about the size of
  each database); kept and reused across runs. Default: a temporary directory.
* ``--no-cache``: re-extract even when a matching index exists in
  ``--work-dir``.
* ``--json PATH``: also write the machine-readable result.
* ``--inline-limit N``: findings per comparison (and matrix rows) listed in the
  report itself (default 2000); above this they go to the appendix and the
  report shows aggregate tables.
* ``--timestamp``: append a ``Generated:`` line (off by default so two runs
  produce identical files).
* ``--java-max-memory``: JVM heap limit (default ``4g``).
* ``--quiet``, ``--verbose``, ``--debug``: progress and log verbosity;
  ``--debug`` also shows full tracebacks.

Distinguishing the exit codes in a script:

.. code-block:: bash

   gcamreader diff ref policy -o report.md
   case $? in
     0) echo "identical structure" ;;
     1) echo "differences found, see report.md" ;;
     *) echo "comparison failed" >&2; exit 1 ;;
   esac
