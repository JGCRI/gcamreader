Installation
============

Requirements
------------

* Python 3.11 or newer.
* A Java runtime (JRE). ``gcamreader`` runs queries against local databases
  using a bundled copy of the GCAM ModelInterface, which requires Java.

.. _verify-java:

Verify your Java installation
-----------------------------

Because the bundled GCAM ModelInterface requires Java, confirm that a Java
runtime is installed and visible on your ``PATH`` before running queries.

.. code-block:: bash

   java -version

You should see output reporting a Java version, for example:

.. code-block:: text

   openjdk version "17.0.10" 2024-01-16
   OpenJDK Runtime Environment (build 17.0.10+7)
   OpenJDK 64-Bit Server VM (build 17.0.10+7, mixed mode, sharing)

If the command reports that ``java`` is not found, install a Java runtime
using one of the options below and ensure it is on your ``PATH``. Any modern
JRE/JDK (for example, `Temurin <https://adoptium.net/>`_ or your platform's
OpenJDK package) will work.

With ``conda`` (cross-platform), install a JRE directly into your
environment:

.. code-block:: bash

   conda install -c conda-forge openjdk

On macOS, use `Homebrew <https://brew.sh/>`_:

.. code-block:: bash

   brew install openjdk

   # Homebrew prints a command to symlink the JDK so the system can find it.
   # For Apple silicon it is typically:
   sudo ln -sfn /opt/homebrew/opt/openjdk/libexec/openjdk.jdk \
       /Library/Java/JavaVirtualMachines/openjdk.jdk

On Windows, use the `winget <https://learn.microsoft.com/windows/package-manager/>`_
package manager (or download an installer from `Adoptium <https://adoptium.net/>`_):

.. code-block:: powershell

   winget install EclipseAdoptium.Temurin.17.JRE

On Linux, install OpenJDK with your distribution's package manager:

.. code-block:: bash

   # Debian / Ubuntu
   sudo apt-get update && sudo apt-get install -y default-jre

   # Fedora / RHEL
   sudo dnf install -y java-17-openjdk

   # Arch Linux
   sudo pacman -S jre-openjdk

After installing, open a new shell and run ``java -version`` again to confirm
it is detected.

Some installations rely on the ``JAVA_HOME`` environment variable. If
``gcamreader`` cannot locate Java even though ``java -version`` works, set
``JAVA_HOME`` to point at your Java installation directory.

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

Upgrade to the latest version
-----------------------------

If you already have ``gcamreader`` installed, upgrade to the latest release
from PyPI with:

.. code-block:: bash

   python -m pip install --upgrade gcamreader

To confirm the installed version after upgrading:

.. code-block:: bash

   python -m pip show gcamreader

If you installed ``gcamreader`` with ``conda``, update it within your
environment instead:

.. code-block:: bash

   pip install --upgrade gcamreader

For a source (editable) install, pull the latest changes and reinstall:

.. code-block:: bash

   cd gcamreader
   git pull
   pip install -e .

Install from source
--------------------

.. code-block:: bash

   git clone https://github.com/JGCRI/gcamreader.git
   cd gcamreader
   pip install -e .

To work on the package, install the development and documentation extras:

.. code-block:: bash

   pip install -e ".[dev,docs]"

Troubleshooting
---------------

``java`` not found or ModelInterface fails to start
   ``gcamreader`` shells out to a bundled Java ModelInterface to run queries.
   If queries fail with errors about Java not being found, confirm
   ``java -version`` works in the same shell you use to run ``gcamreader``
   (see :ref:`Verify your Java installation <verify-java>` above). If Java is
   installed but still not detected, set the ``JAVA_HOME`` environment
   variable to your Java installation directory and open a new terminal.

``pip`` installs into the wrong environment
   If ``import gcamreader`` fails after installation, you may have installed
   into a different Python than the one you are running. Verify the active
   interpreter and that the package is present:

   .. code-block:: bash

      which python      # macOS / Linux
      where python      # Windows
      python -m pip show gcamreader

   Reactivate your virtual environment and reinstall if the paths do not
   match. Using ``python -m pip install ...`` ensures ``pip`` targets the
   currently active interpreter.

``ModuleNotFoundError`` for pandas or other dependencies
   This usually means dependencies were not installed into the active
   environment. Reinstall ``gcamreader`` into the activated environment:

   .. code-block:: bash

      python -m pip install --upgrade --force-reinstall gcamreader

SSL or certificate errors during ``pip install``
   These typically indicate a network or proxy issue rather than a problem
   with ``gcamreader``. Ensure ``pip`` is up to date and retry:

   .. code-block:: bash

      python -m pip install --upgrade pip

Permission errors during installation
   Avoid using ``sudo pip``. Instead, install into a virtual environment (see
   `Set up a virtual environment`_) or use ``pip install --user gcamreader``.

Editable install (``pip install -e .``) fails
   Make sure you are running the command from the repository root (the
   directory containing ``pyproject.toml``) and that ``pip`` is current with
   ``python -m pip install --upgrade pip``.

If you encounter an issue not covered here, please open an issue on the
`GitHub issue tracker <https://github.com/JGCRI/gcamreader/issues>`_.
