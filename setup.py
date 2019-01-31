import sys
from setuptools import setup


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

# [0] major version, [1] minor, [2] mirco, [3] releaselevel, [4] serial
reader_info = sys.version_info

# account for version dependency for subprocess
if (reader_info[0] == 3) and (reader_info[1] >= 5):
    pass

elif (reader_info[0] == 3) and (reader_info[1] < 5):
    msg = """DEPRECIATION:  gcam_reader will soon only support >= Python 3.5. Please upgrade
             your Python version."""

    requirements.append("subprocess.run>=0.0.8")

    raise PendingDeprecationWarning(msg)

elif (reader_info[0] == 2) and (reader_info[1] in (6, 7)):
    msg = """DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020.
             Please upgrade your Python as Python 2.7 won't be maintained after that date.
             A future version of pip will drop support for Python 2.7.
             gcam_reader will soon only support >= Python 3.5. Please upgrade
             your Python version."""

    raise PendingDeprecationWarning(msg)

else:
    msg = """DEPRECATION: Your Python version {}.{} is depreciated Python 2.7 will reach the end of its life on January 1st, 2020.
             Please upgrade your Python as Python 2.7 won't be maintained after that date.
             A future version of pip will drop support for Python 2.7.
             gcam_reader will soon only support >= Python 3.5. Please upgrade
             your Python version.""".format(reader_info[0], reader_info[1])

    raise DeprecationWarning(msg)

setup(
    name="gcam_reader",
    version="0.5.0",
    description="Tools for importing GCAM output data",
    url="https://github.com/JGCRI/gcam_reader",
    author="Robert Link",
    author_email="robert.link@pnnl.gov",
    packages=["gcam_reader"],
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False
    )