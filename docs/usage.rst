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
