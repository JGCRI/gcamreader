import sys
from setuptools import setup


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

# [0] major version, [1] minor, [2] mirco, [3] releaselevel, [4] serial
reader_info = sys.version_info

# account for version dependency for subprocess
if (reader_info[0] == 3) and (reader_info[1] >= 5):
    pass

else:
    msg = """\nDEPRECATION: Your Python version {}.{} is depreciated due to Python 2.7 reaching the end of its life on 
                January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date.
                A future version of pip will drop support for Python 2.7.
                \ngcam_reader will now only support >= Python 3.5. Please upgrade your Python version.\n""".format(reader_info[0], reader_info[1])

    raise DeprecationWarning(msg)

setup(
    name="gcam_reader",
    version="1.0.0",
    description="Tools for importing GCAM output data",
    url="https://github.com/JGCRI/gcam_reader",
    author="Robert Link",
    author_email="robert.link@pnnl.gov",
    packages=["gcam_reader"],
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False
    )
