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

import sys

from argparse import ArgumentParser

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from cli.helper import help
from cli.start_transfer import start_transfer

parser = ArgumentParser(description="TODO")

parser.add_argument('--configuration', '-C', nargs=1,
    # user input is stored in a list with a single element, a list default
    # makes the argument simpler to process
    default=['~/.shepherd'],
    help="Specify a custom path to the shepherd configuration file, or directory containing multiple configuration files.")
parser.add_argument('-v', nargs=1, action='append',
    help="VARIABLE=VALUE\nReplace instances of 'VARIABLE' in templated configuration files with 'VALUE'. Substitute multiple variables by using multiple '-v' flags.")
parser.add_argument('action', nargs='*',
    help="Action or query for shepherd. See 'shepherd help actions' for more.")

def declare_prep_subparser():
    subparsers = parser.add_subparsers(help="For internal use only.")

    parser_prep = subparsers.add_parser("_prep")
    # label of named route in config
    parser_prep.add_argument('--route', nargs='?')
    # label of source filesystem in config
    parser_prep.add_argument('--fssource', nargs='?')
    # label of target filesystem in config
    parser_prep.add_argument('--fstarget', nargs='?')

def declare_work_subparser():
    # TODO
    pass

def prepare_config(parsed_args:T.Any, args:T.List[str]) -> T.Dict[str, T.Any]:
    """Converts argument parser namespace into an organised dictionary."""
    if parsed_args.v is not None:
        variable_dict = {}
        for variable in parsed_args.v:
            variable_name, variable_value = variable[0].split("=")
            variable_dict[variable_name] = variable_value
    else:
        variable_dict = None

    configuration = T.Path(parsed_args.configuration[0]).expanduser()

    # original terminal input string
    command = " ".join(args)

    config = {
        "variables": variable_dict,
        "configuration": configuration,
        "command": command
    }

    # extra fields for prep and work modes
    if "route" in vars(parsed_args):
        config["route"] = parsed_args.route

    return config

def main(*args:str) -> None:
    """ CLI entrypoint """
    # seems to work even when main() is called from the top-level
    # shepherd executable
    mode = "user"
    if "_prep" in args:
        declare_prep_subparser()
        parsed_args = parser.parse_args()
        mode = "prep"
    elif "_work" in args:
        declare_work_subparser()
        parsed_args = parser.parse_args()
        mode = "work"
    else:
        parsed_args = parser.parse_args()


    if len(parsed_args.action) == 0:
        print("No action specified.")
        exit(1)

    if parsed_args.action[0] == "help":
        help(parsed_args.action)

    print(parsed_args)
    print("\n")

    configuration = prepare_config(parsed_args, args)

    print(configuration)
    print("\n")


    if mode == "user":
        start_transfer(parsed_args.action, configuration)
    elif mode == "prep":
        prepare_state_from_fofn(parsed_args, configuration)
