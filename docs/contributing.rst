Contributing
============

Contributions are welcome. To set up a development environment:

.. code-block:: bash

   git clone https://github.com/JGCRI/gcamreader.git
   cd gcamreader
   pip install -e ".[dev,docs]"

Run the test suite and the linters before opening a pull request:

.. code-block:: bash

   pytest
   ruff check .
   black --check .

Building the documentation
---------------------------

.. code-block:: bash

   sphinx-build -b html docs docs/_build/html

Regression baseline
--------------------

The ``benchmarks/`` directory contains a behavioral baseline captured from the
previous release. The regression tests there ensure that changes continue to
produce identical query outputs. Run them with:

.. code-block:: bash

   pytest benchmarks/test_regression.py
