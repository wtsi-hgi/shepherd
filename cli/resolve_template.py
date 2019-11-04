"""
Copyright (c) 2019 Genome Research Limited

Author: Filip Makosza <fm12@sanger.ac.uk>

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

import re
from jinja2 import Template, Environment

from common import types as T
from common.templating import templating_factory, Jinja2Templating
from lib.planning.templating import _filters


def resolve_template(yaml_file:T.Path, temp_file:T.Path, vars:T.Dict[str, str]) -> None:
    # TODO: Replace this with something that doesn't use the filesystem
    """Reads a YAML configuration file with template strings and resolves
    them based on variables passed by the user at runtime.
    @param yaml_file Path object pointing at YAML config file
    @param temp_file Path object pointing at a temporary file to write the resolved config to
    @param vars Dictionary of variable:value mappings"""

    env = Environment()
    env.filters.update(_filters)

    # stops processing variables when in a template: inline code block
    ignore = re.compile("^[ ]*[-]*[ ]*template:")
    # continues processing variables when another YAML declaration is seen
    unignore = re.compile("^[ ]*[-]*[ ]*[a-zA-Z-_]+:")
    template = re.compile("{{[\w\d.|\- ]*}}")

    ignoring:T.Bool = False

    with open(yaml_file, 'r') as src_file, open(temp_file, 'w') as temp_file:
        for line in src_file:
            if ignoring:
                if unignore.search(line):
                    ignoring = False
            if not ignoring:
                # ignore check needs to come second, because the unignore
                # regex matches all declarations, while the ignore regex
                # only matches a 'template:' declaration specifically.
                if ignore.search(line):
                    ignoring = True

            if ignoring:
                temp_file.write(line)
            if not ignoring:
                template = env.from_string(line)
                rendered_line = template.render(**vars)
                temp_file.write(rendered_line+"\n")
