# -*- coding: utf-8 -*-

from setuptools import find_packages

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

INSTALL_REQUIRES = [
    "loguru>=0.5.0",
    "confluent-kafka>=1.6.0",
    "fastapi>=0.68.0",
    "uvicorn[standard]>=0.14.0",
    "pydantic>=1.0.0",
    "PyYAML>=5.4.*",
]

DEV_REQUIRES = [
    "flake8",
    "black",
    "isort",
    "mypy",
    "pytest==6.2.5",
    "pytest-mock",
    "assertpy",
]

setup(
    name="eventbus",
    version="0.1.0",
    author="Benn Ma",
    author_email="bennmsg@gmail.com",
    description="A reliable event/message hub for boosting Event-Driven architecture & big data ingestion.",
    long_description=readme,
    long_description_content_type="text/markdown",
    python_requires=">=3.7.0",
    url="https://github.com/thenetcircle/event-bus-3",
    project_urls={
        "Bug Tracker": "https://github.com/thenetcircle/event-bus-3/issues",
    },
    license="Apache License, Version 2.0",
    packages=find_packages(include=("eventbus*",)),
    install_requires=INSTALL_REQUIRES,
    extras_require={
        "dev": DEV_REQUIRES,
    },
    keywords=("event-bus eventbus eventhub messagehub event-driven micro-services"),
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
