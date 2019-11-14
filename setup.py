"""
Copyright (c) 2019 Genome Research Limited

Author: Christopher Harrison <ch12@sanger.ac.uk>

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see https://www.gnu.org/licenses/
"""

from pathlib import Path
from setuptools import setup


# FIXME The dummy CLI instantiates objects that depend on the external
# libraries defined herein, so we can't import it here without creating
# an unresolvable loop. The "proper" CLI must address this.
from lib import __version__ as lib_version
from cli import __version__ as cli_version

version = f"{cli_version}/{lib_version}"


setup(
    name="shepherd",
    version=version,

    description="Filesystem-agnostic distributed copy tool",
    long_description=Path("README.md").read_text(),

    author="Christopher Harrison",
    license="GPLv3",

    packages=["shepherd"],
    python_requires=">=3.7",
    install_requires=[
        # TODO Insert more here...
        "Jinja2",
        "PyYAML",
        "pyscopg2"
    ],

    classifiers=[
        # TODO Insert more here...
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Programming Language :: Python :: 3.7"
    ]
)
