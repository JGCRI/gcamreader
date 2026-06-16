Installation
============

Requirements
------------

* Python 3.11 or newer.
* A Java runtime (JRE). ``gcamreader`` runs queries against local databases
  using a bundled copy of the GCAM ModelInterface, which requires Java.

Set up a virtual environment
----------------------------

It is recommended to install ``gcamreader`` into an isolated virtual
environment to avoid conflicts with other packages on your system.

Using the built-in ``venv`` module:

.. code-block:: bash

   # Create a virtual environment in a directory named ".venv".
   python -m venv .venv

   # Activate it (macOS / Linux).
   source .venv/bin/activate

   # Activate it (Windows PowerShell).
   .venv\Scripts\Activate.ps1

Alternatively, with ``conda``:

.. code-block:: bash

   conda create -n gcamreader python=3.11
   conda activate gcamreader

Once the environment is activated, install ``gcamreader`` using one of the
methods below. To leave the environment when you are finished, run
``deactivate`` (``venv``) or ``conda deactivate`` (``conda``).

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
