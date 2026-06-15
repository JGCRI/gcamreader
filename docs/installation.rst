Installation
============

Requirements
------------

* Python 3.11 or newer.
* A Java runtime (JRE). ``gcamreader`` runs queries against local databases
  using a bundled copy of the GCAM ModelInterface, which requires Java.

Install from PyPI
-----------------

.. code-block:: bash

   pip install gcamreader

Install from source
--------------------

.. code-block:: bash

   git clone https://github.com/JGCRI/gcamreader.git
   cd gcamreader
   pip install -e .

To work on the package, install the development and documentation extras:

.. code-block:: bash

   pip install -e ".[dev,docs]"
