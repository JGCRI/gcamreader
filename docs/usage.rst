Usage
=====

Query a local database
-----------------------

.. code-block:: python

   import gcamreader

   # Connect to a local GCAM database (the parent directory and database name).
   conn = gcamreader.LocalDBConn("path/to/output", "database_basexdb")

   # List the scenarios contained in the database.
   scenarios = conn.listScenariosInDB()

   # Parse a GCAM queries XML file and run the first query.
   queries = gcamreader.parse_batch_query("Main_queries.xml")
   df = conn.runQuery(queries[0])

Run many queries at once
------------------------

.. code-block:: python

   import gcamreader

   results = gcamreader.importdata(
       "path/to/output/database_basexdb",
       "Main_queries.xml",
   )

   # results is a dict keyed by query title, with DataFrame values.
   for title, frame in results.items():
       print(title, None if frame is None else frame.shape)

Query a remote database
------------------------

.. code-block:: python

   import gcamreader

   conn = gcamreader.RemoteDBConn(
       dbfile="database_basexdb",
       username="user",
       password="secret",
       address="localhost",
       port=8984,
   )
   df = conn.runQuery(gcamreader.parse_batch_query("Main_queries.xml")[0])

Filtering scenarios and regions
-------------------------------

Both :meth:`gcamreader.LocalDBConn.runQuery` and
:meth:`gcamreader.RemoteDBConn.runQuery` accept ``scenarios`` and ``regions``
arguments to filter the results. When ``regions`` is omitted, the region filter
parsed from the query XML is used.

.. code-block:: python

   df = conn.runQuery(
       queries[0],
       scenarios=["Reference"],
       regions=["USA", "China"],
   )

Compare the structure of two databases
--------------------------------------

:func:`gcamreader.compare_databases` compares which elements and metadata
attributes exist in two or more local output databases, ignoring numeric
values. See :doc:`structure_comparison` for what is compared and how to read
the result.

.. code-block:: python

   import gcamreader

   paths = ["output/database_basexdb_ref", "output/database_basexdb_policy"]

   # Which runs does each database hold? Only needed to pick one of several.
   for ref in gcamreader.list_scenarios(paths[1]):
       print(ref.label, ref.version)

   result = gcamreader.compare_databases(
       paths,
       scenarios={2: "Policy@2026-01-07T00:00:00-07:00"},  # optional selector
       work_dir="~/.cache/gcamreader-structure",           # keep indexes for reuse
       progress=print,                                      # optional status messages
   )

   print(result.has_differences)
   comparison = result.comparisons[0]          # candidate 2 against the baseline
   print(comparison.summary.counts)            # findings per kind
   for finding in comparison.findings[:10]:
       print(finding.kind.value, finding.path, finding.members)
       # finding.segments is the path as data: (tag, identity pairs) per level

   # Markdown report (plus appendix when lists are long) and JSON.
   from gcamreader.structure import write_json, write_report

   write_report(result, "report.md")
   write_json(result, "result.json")   # same content as result.to_dict()

Rules can be customised with :class:`gcamreader.ComparisonRules`:

.. code-block:: python

   rules = gcamreader.ComparisonRules.from_toml("rules.toml")
   rules = rules.with_overrides(ignored_attributes=["fillout"])
   result = gcamreader.compare_databases(paths, rules=rules)

All errors raised by the comparison derive from
:class:`gcamreader.StructureError`.
