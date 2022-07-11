import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="batchout",
    version="0.3.0",
    author="Ilia Khaustov",
    description="Framework for building data pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ilia-khaustov/batchout",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    entry_points={
        "console_scripts": [
            "batchout = batchout.cli:main",
        ],
    },
    python_requires=">=3.9",
    extras_require={
        "xpath": ["lxml==4.8.0"],
        "jsonpath": ["jsonpath_rw==1.4.0"],
        "postgres": ["psycopg==3.0.14"],
        "cli": ["pyyaml==6.0"],
    },
)
