from setuptools import setup

setup(
    name="gcam_reader",
    version="0.1.1",
    description="Tools for importing GCAM output data",
    url="https://github.com/JGCRI/gcam_reader",
    author="Robert Link",
    author_email="robert.link@pnnl.gov",
    packages=["gcam_reader"],
    install_requires=[
        "requests>=2.18.4",
        "pandas>=0.20",
    ],
    include_package_data=True,
    zip_safe=False
    )

