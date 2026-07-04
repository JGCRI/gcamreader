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
