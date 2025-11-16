"""Setup file for PyDeequ Polars."""

from setuptools import setup, find_packages

with open("PYTHON_README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="pydeequ-polars",
    version="0.1.0",
    author="PyDeequ Polars Contributors",
    description="Data quality validation library for Polars DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/pydeequ-polars",
    packages=find_packages(exclude=["tests", "examples"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Software Development :: Testing",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=[
        "polars>=0.19.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "mypy>=1.0.0",
            "ruff>=0.1.0",
        ],
    },
    package_data={
        "pydeequ_polars": ["py.typed"],
    },
)
