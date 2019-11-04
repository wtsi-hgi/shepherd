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

from argparse import ArgumentParser

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from cli.helper import help

parser = ArgumentParser(description="TODO")
parser.add_argument('--settings', '-S', nargs=1,
    default=T.Path('~/.shepherdrc'),
    help="Specify a custom path to the shepherd settings file.")
parser.add_argument('--configuration', '-C', nargs=1,
    default=T.Path('~/.shepherd'),
    help="Specify a custom path to the shepherd configuration file, or directory containing multiple configuration files.")
parser.add_argument('-v', nargs=1, action='append',
    help="VARIABLE=VALUE\nReplace instances of 'VARIABLE' in templated configuration files with 'VALUE'. Substitute multiple variables by using multiple '-v' flags.")
parser.add_argument('--variables', nargs=1,
    help="Specify the path to a YAML file containing definitions for variables in templated configuration files.")
parser.add_argument('action', nargs='+')

def organise_vars(vars:T.List[str]) -> T.Dict[str, str]:
    """Converts list of strings 'VAR=VAL' to a dictionary with VAR:VAL
    mappings."""
    variable_dict:T.Dict[str, str] = {}

    for variable in vars:
        # each string is contained in a one-element list
        variable_name, variable_value = variable[0].split("=")
        variable_dict[variable_name] = variable_value

    return variable_dict

def main(*args:str) -> None:
    """ CLI entrypoint """
    # seems to work even when main() is called from the top-level
    # shepherd executable
    args = parser.parse_args()

    vars = organise_vars(args.v)
    
    configuration = {
        "settings": T.Path(args.settings),
        "configuration": T.Path(args.configuration),
        "given_variables": vars,
        "variable_file": T.Path(args.variables)
    }

    if args.action[0] == "help":
        help(args.action, configuration)
