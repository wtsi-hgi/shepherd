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

from yaml import safe_load, FullLoader

from tempfile import NamedTemporaryFile
from common import types as T
from common.models.graph import Route
from cli.resolve_template import resolve_templates
from lib import api
from lib.planning.templating import transfer_script
from lib.planning.types import TransferRoute, PolynomialComplexity, FilesystemVertex

class InvalidConfigurationError(Exception):
    """Raised when an unrecognised value is found in a config file."""


def validate_options(options:T.Dict[str, T.Any], type:str, name:str) -> T.Dict[str, T.Any]:
    """
    Checks whether an option dictionary is suitable for
    instantiating a particular class.

    @param options Dictionary of the form {option_name: option_value}
    @param type Name of a Register, currently either 'transformer' or 'filesystem'
    @param name The name of a particular Register entry.
    @return Dictionary with mapping {option: value}
    """
    option_dict:T.Dict[str, T.Any] = {}
    valid_options:T.Dict[str, T.Type] = {}

    if type == "transformer":
        for argument in api.transformers[name].arguments:
            valid_options[argument.name] = argument.type

    elif type == "filesystem":
        for argument in api.filesystems[name].arguments:
            valid_options[argument.name] = argument.type

    else:
        raise InvalidConfigurationError(f"Can't validate options for {type}, type not recognised!")

    for option in options:
        value = options[option]

        if option not in valid_options.keys():
            raise InvalidConfigurationError(f"{type} of type '{name}' does not take option '{option}'!")

        try:
            option_dict[option] = valid_options[option](value)
        except ValueError:
            raise InvalidConfigurationError(f"File system option {option} is of type {type(value)}, which can't be cast to {valid_options[option]}.")

    return option_dict

def produce_filesystem(data:T.Dict[str, T.Any]) -> T.Any:
    """Takes a config dictionary describing a filesystem object, returns
    an object with those parameters."""
    driver:str = data["driver"]

    if driver not in api.filesystems.keys():
        raise InvalidConfigurationError(f"File system driver '{driver}' not recognised!")

    option_dict:T.Dict[str, T.Any] = {}

    try:
        option_dict["name"] = data["name"]
    except KeyError:
        # object will use default value when initialised
        pass

    # for filesystems, the driver marks the Register index
    option_dict.update(
        validate_options(data["options"], "filesystem", driver))

    try:
        filesystem_object = api.filesystems[driver](**option_dict)
    except Exception as e:
        raise InvalidConfigurationError(f"Error instantiating filesystem '{name}':\n{e}")

    return filesystem_object

def produce_transformation(data:T.Dict[str, T.Any]):
    """Takes a config dictionary describing a transformation object, returns an
    object with those parameters."""
    name = data["name"]

    if name not in api.transformers.keys():
        raise InvalidConfigurationError(f"Transformation '{name}' not recognised!")

    if "options" not in data.keys():
        return api.transformers[name].callable

    option_dict:T.Dict[str, T.Any] = {}

    # for transformations, the name marks the Register index
    option_dict.update(
        validate_options(data["options"], "transformer", name))

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
        template = transfer_script(template)

    option_dict["templating"] = template

    try:
        option_dict["source"] = FilesystemVertex(filesystems[data["source"]])
    except KeyError:
        raise InvalidConfigurationError(f"Transfer source named {data['source']} not found in filesystems list.")

    try:
        option_dict["target"] = FilesystemVertex(filesystems[data["target"]])
    except KeyError:
        raise InvalidConfigurationError(f"Transfer target named {data['target']} not defined in filesystems list.")

    try:
        option_dict["cost"] = PolynomialComplexity(data["cost"])
        if option_dict["cost"] < 0:
            raise InvalidConfigurationError(f"Cost of transfer {data['name']} can't be less than 0.")
    except KeyError:
        # cost is optional, just ignore it if it isn't present
        pass

    transfer_object = TransferRoute(**option_dict)

    for transformation in data["transformations"]:
        transfer_object += produce_transformation(transformation)

    return transfer_object

def produce_route(data:T.Dict[str, T.Any], transfers:T.Dict[str, T.Any]) -> T.Any:
    """Takes a config dictionary describing a route object, returns an  object
    with those parameters."""
    name = data["name"]

    route:Route = []

    valid_transfers:T.List[str] = []
    for transfer in transfers:
        valid_transfers.append(transfer)

    for step in data["route"]:
        if step["name"] not in valid_transfers:
            raise InvalidConfigurationError(f"Named route step named {step['name']} not defined in transfers list.")

        transfer = transfers[step["name"]]
        try:
            for transformation in step["transformations"]:
                transfer += produce_transformation(transformation)
        except KeyError:
            # no transformations defined, just move on
            pass

        if len(route) > 0:
            if transfer.source != route[-1].target:
                raise InvalidConfigurationError(f"Named route step named {step['name']} is not the target of its immediately preceding step!")

        route.append(transfer)

    return route

def read_yaml(yaml_file:T.Path, vars:T.Dict[str, str]) -> T.Dict[str, T.Any]:
    """
    Reads a YAML configuration file and validates each field. If the file is
    considered valid, an appropriate dictionary {field_name: Object} is
    returned.

    @param yaml_file Path object pointing at YAML config file
    @param vars Dictionary of variable: value mappings
    @return dictionary of string: object mappings
    """

    object_dict: T.Dict[str, T.Any] = {}

    data = resolve_templates(yaml_file, vars)

    filesystems = {}
    transfers = {}
    routes = {}

    for entry in data["filesystems"]:
        name = entry["name"]
        filesystems[name] = produce_filesystem(entry)

    if len(filesystems) == 0:
        raise InvalidConfigurationError("No filesystems defined!")

    for entry in data["transfers"]:
        name = entry["name"]
        transfers[name] = produce_transfer(entry, filesystems)

    if len(transfers) == 0:
        raise InvalidConfigurationError("No transfers defined!")

    for entry in data["named_routes"]:
        name = entry["name"]
        routes[name] = produce_route(entry, transfers)

    for key in data:
        if key not in ("filesystems", "transfers", "named_routes"):
            raise InvalidConfigurationError(f"Unrecognised category definition '{key}' in configuration file!")

    object_dict["filesystems"] = filesystems
    object_dict["transfers"] = transfers
    object_dict["named_routes"] = routes
    return object_dict
