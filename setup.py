from setuptools import setup, find_packages


setup(
    name="gcamreader",
    version="1.2.3",
    python_requires=">=3.6",
    packages=find_packages(),
    description="Tools for importing GCAM output data",
    url="https://github.com/JGCRI/gcam_reader",
    license='BSD 2-Clause',
    author="Robert Link",
    author_email="robert.link@pnnl.gov",
    install_requires=[
        "requests~=2.20.0",
        "pandas~=1.2.4",
        "lxml>=4.6.3"
    ],
    include_package_data=True,
    zip_safe=False
    )
