import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="batchout",
    version="0.1.0",
    author="Ilia Khaustov",
    author_email="ilya.khaustov@gmail.com",
    description="Framework for building data pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ilia-khaustov/batchout",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    install_requires=[
        'arrow',
        'requests',
        'lxml',
        'jsonpath_rw',
        'kafka-python',
        'psycopg2-binary',
    ],
    python_requires='>=3.6',
)
