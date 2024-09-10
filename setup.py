"""
setup.py: distutils/setuptools install script.
"""

#!/usr/bin/env python

from setuptools import setup

REQUIRES = [
    "Django>=5.1,<5.2",
    "boto3>=1.35,<1.36",
]

try:
    with open("README.md", encoding="utf-8") as f:
        LONG_DESCRIPTION = f.read()
except FileNotFoundError:
    LONG_DESCRIPTION = ""

setup(
    name="django-sqlite-efs",
    version="0.1.0",
    author="Efficient Solutions LLC",
    author_email="contact@efficient.solutions",
    description="Django database backend for SQLite on Amazon EFS",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/efficient-solutions/django-sqlite-efs",
    packages=["django_sqlite_efs"],
    license="MIT",
    install_requires=REQUIRES,
    python_requires=">= 3.11",
    keywords=[
        "Django", "SQLite", "AWS", "Amazon EFS", "AWS Lambda",
        "Amazon DynamoDB", "Serverless"
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ]
)
