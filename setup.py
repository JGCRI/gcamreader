from setuptools import setup

setup(
    name="gcam_reader",
    version="0.1",
    description="Tools for importing GCAM output data",
    url="https://github.com/JGCRI/gcam_reader",
    author="Robert Link",
    author_email="robert.link@pnnl.gov",
    packages=["gcam_reader"],
    install_requires=[
        "requests"
    ],
    zip_safe=False
    )

