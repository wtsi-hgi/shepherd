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

from yaml import load, FullLoader
from tempfile import NamedTemporaryFile
from common import types as T
from lib import api
from lib.planning.types import TransferRoute, PolynomialComplexity
from lib.planning.templating import transfer_script, load_template

class InvalidConfigurationError(BaseException):
    """Raised when an unrecognised value is found in the config file"""
    pass


def produce_filesystem(data:T.Dict[str, T.Any]) -> T.Any:
    """Takes a config dictionary describing a filesystem object, returns
    an object with those parameters."""
    driver:str = data["driver"]

    option_dict:T.Dict[str, T.Any] = {}

    try:
        option_dict["name"] = data["name"]
    except KeyError:
        # object will use default value when initialised
        pass

    if driver not in api.filesystems.keys():
        raise InvalidConfigurationError(f"File system driver '{driver}' not recognised!")

    valid_options:T.Dict[str, T.Type] = {}
    # finds names and types of all defined options for file system driver
    for argument in api.filesystems[driver].arguments:
        opt_name = argument.name
        opt_type = argument.type

        valid_options[opt_name] = opt_type

    for option in data["options"]:
        # TODO: verify that each option is a simple key:value pair

        value = data["options"][option]

        if option not in valid_options.keys():
            raise InvalidConfigurationError(f"File system option {option} not recognised!")

        try:
            # tries to cast the value to the option's expected type
            option_dict[option] = valid_options[option](value)
        except ValueError:
            raise InvalidConfigurationError(f"File system option '{option}' is of type {type(value)}, which can't be cast to {valid_options[option]}.")

    try:
        filesystem_object = api.filesystems[driver](**option_dict)
    except Exception as e:
        raise InvalidConfigurationError(f"Error instantiating filesystem '{name}':\n{e}")

    return filesystem_object

def produce_transformation(data:T.Dict[str, T.Any]):
    """Takes a config dictionary describing a transformation object, returns an
    object with those parameters."""
    name = data["name"]
    opts = True

    try:
        options = data["options"]
    except KeyError:
        opts = False

    if not opts:
        return api.transformers[name]()

    valid_options:T.Dict[str, T.Type] = {}
    # finds names and types of all defined options for the transformation
    for argument in api.transformers[name].arguments:
        opt_name = argument.name
        opt_type = argument.type

        valid_options[opt_name] = opt_type

    option_dict:T.Dict[str, T.Any] = {}
    for option in options:
        if len(options[option]) > 1:
            raise InvalidConfigurationError(f"Transformation option {option} can't have a list of values.")

        value = options[option]

        if option not in valid_options.keys():
            raise InvalidConfigurationError(f"Transformation option {option} not recognised!")

        try:
            # tries to cast the value to the option's expected type
            option_dict[option] = valid_options[option](value)
        except ValueError:
            raise InvalidConfigurationError(f"Transformation option '{option}' is of type {type(value)}, which can't be cast to {valid_options[option]}.")

    try:
        transformation_object = api.transformers[name](**option_dict)
    except Exception as e:
        raise InvalidConfigurationError(f"Error instantiating transformation '{name}':\n{e}")

    return transformation_object

def produce_transfer(data:T.Dict[str, T.Any], filesystems:T.Dict[str, T.Any]) -> T.Any:
    """Takes a config dictionary describing a transfer object, returns an
    object with those parameters."""
    option_dict:T.Dict[str, T.Any] = {}
    name = data["name"]

    template:str = data["template"]

    # TODO: Find better method to detect if a template is a path or inline script
    if template[0:2] != "#!":
        try:
            template = transfer_script(load_template(T.Path(template)))
        except FileNotFoundError:
            raise InvalidConfigurationError(f"Template script {template} not found. If the template is supposed to be inline code, make sure to start it with an interpreter directive such as '#!/bin/env bash'.")
    else:
        with NamedTemporaryFile() as temp_file:
            temp_file.write(bytes(template, "UTF-8"))
            template = transfer_script(load_template(T.Path(temp_file.name)))
    option_dict["templating"] = template

    try:
        option_dict["source"] = filesystems[data["source"]]
    except KeyError:
        raise InvalidConfigurationError(f"Transfer source named {data['source']} not found in filesystems list.")

    try:
        option_dict["target"] = filesystems[data["target"]]
    except KeyError:
        raise InvalidConfigurationError(f"Transfer target named {data['target']} not found in filesystems list.")

    try:
        option_dict["cost"] = int(data["cost"])
        if option_dict["cost"] < 0:
            InvalidConfigurationError(f"Cost of transfer {data['name']} can't be less than 0.")
    except KeyError:
        # cost is optional, just ignore it if it isn't present
        pass

    transfer_object = TransferRoute(**option_dict)

    for transformation in data["transformations"]:
        transfer_object += produce_transformation(transformation)

    return transfer_object

def read_yaml(yaml_file:T.Path) -> T.Dict[str, T.Any]:
    """
    Reads a YAML configuration file and validates each field. If the file is
    considered valid, an appropriate dictionary {field_name: Object} is
    returned.

    @param yaml_file Path object pointing at YAML config file
    @return dictionary of string: object mappings
    """

    object_dict: T.Dict[str, T.Any] = {}

    with open(yaml_file) as file:
        data = load(file, FullLoader)

        filesystems = {}
        transfers = {}

        for key in data:
            if key == "filesystems":
                if len(filesystems) > 0:
                    raise InvalidConfigurationError("Multiple 'filesystems' categories defined! Combine them into a single list.")

                for entry in data["filesystems"]:
                    name = entry["name"]
                    filesystems[name] = produce_filesystem(entry)

            elif key == "transfers":
                if len(filesystems) == 0:
                    raise InvalidConfigurationError("No filesystems defined, or filesystem definitions placed after transfer definitions!")

                for entry in data["transfers"]:
                    name = entry["name"]
                    transfers[name] = produce_transfer(entry, filesystems)

            elif key == "named_routes":
                pass

            else:
                raise InvalidConfigurationError(f"Option category {key} not recognised!")

    object_dict["filesystems"] = filesystems
    object_dict["transfers"] = transfers
    return object_dict
