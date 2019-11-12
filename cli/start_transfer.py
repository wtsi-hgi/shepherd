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

from common import types as T

from cli.yaml_parser import read_yaml
from common.logging import log, Level
from common.models.graph import Graph, Edge
from lib.state.native.db import NativeJob, create_root
from lib.state.types import JobStatus, DataNotReady, WorkerRedundant
from lib.execution.lsf import LSF, LSFSubmissionOptions
from lib.execution.types import Job
from lib.planning.types import TransferRoute, PolynomialComplexity, FilesystemVertex

class QueryError(Exception):
    """Raised when an unrecognised query is received from the user"""

# TODO: clean up the self-call parameters! Currently extremely messy

def parse_action(action:T.List[str]) -> T.Dict[str, T.Any]:
    """Parse action input and return dictionary of relevant values."""
    # TODO: implement actual query language parser
    query:T.Dict[str, T.Any] = {}

    if action[0] == "through" and action[2] == "using":
        query["route"] = action[1]
        query["fofn"] = action[3]
    elif action[0] == "from" and action[2] == "to" and action[4] == "using":
        query["source"] = action[1]
        query["target"] = action[3]
        query["fofn"] = action[5]
    else:
        raise QueryError(f"Query '{' '.join(action)}' not recognised.")

    return query

def start_transfer(action:T.List[str], config:T.Dict[str, T.Any]) -> None:
    """
    Starts the shepherd file transfer process based on user input and program
    configuration.

    @param action List of user input strings
    @param config Dictionary of various shepherd configuration values
    """
    # this has to be here in full so the user can see config errors immediately
    transfer_objects = read_yaml(config["configuration"], config["variables"])

    query = parse_action(action)

    # this has to be here so the user can see query errors immediately
    if "route" in query.keys():
        try:
            route = transfer_objects["named_routes"][query["route"]]
        except KeyError:
            raise QueryError(f"Named route '{query['route']}' is not defined in the configuration file.")

    elif "source" in query.keys() and "target" in query.keys():
        try:
            source = FilesystemVertex(
                transfer_objects["filesystems"][query["source"]] )
            target = FilesystemVertex(
                transfer_objects["filesystems"][query["target"]] )
        except KeyError:
            raise QueryError(f"Either '{query['source']}' or '{query['target']}' is not defined in the configuration file.")

    working_dir = create_root(T.Path("."))

    # gets location of the binary from command line invocation
    binary = T.Path( config["command"][0] ).resolve()
    arguments = config["command"][1:]

    # TODO: generalise this once more executors are added
    lsf = transfer_objects["executor"]
    lsf_options = transfer_objects["phases"]["preparation"]

    fofn = T.Path(query["fofn"]).resolve()

    if "route" in query.keys():
        prep = f"--route {query['route']}"
    elif "source" in query.keys():
        prep = f"--fssource {query['source']} --fstarget {query['target']}"

    v_indices = [i for i,val in enumerate(arguments) if val=="-v"]
    variables = ""
    for v in v_indices:
        variables += f"-v {arguments[v+1]}"

    # the _prep keyword has to be put after the user arguments (variables,
    # config) because it's the trigger word for the _prep subparser in cli/main
    job = Job(f'"{binary}" {variables} --configuration {config["configuration"]} _prep  {prep} --fofn {fofn} --stateroot {working_dir}')
    job.stdout = job.stderr = working_dir / "prep.log"

    prep_job, *_ = lsf.submit(job, lsf_options)

    log(f"Preparation job submitted with ID {prep_job.job}")
    log(f"State and logs will reside in {working_dir}")

def _print_status(status:JobStatus) -> None:
    log(f"* Pending:    {status.pending}")
    log(f"* Running:    {status.running}")
    log(f"* Failed:     {status.failed}")
    log(f"* Succeeded:  {status.succeeded}")

def prepare_state_from_fofn(config:T.Dict[str, T.Any]) -> None:
    """
    Reads file names in from a fofn file and starts a set of jobs that will
    transfer those files from one filesystem to another. Should only be used
    when shepherd is executed by 'start_transfer()'

    @param config Dictionary of shepherd configuration values
    """
    transfer_objects = read_yaml(config["configuration"], config["variables"])

    transfer:T.TransferRoute = None

    if "route" in config.keys():
        transfer = transfer_objects["named_routes"][config["route"]]
    elif "filesystems" in config.keys():
        fs = config["filesystems"]
        source = FilesystemVertex(
            transfer_objects["filesystems"][fs["source"]])
        target = FilesystemVertex(
            transfer_objects["filesystems"][fs["target"]])

        graph = Graph()
        edge = Edge(source, target)
        graph += edge
        transfer = graph.route(source, target)

    job = NativeJob(config["stateroot"])
    job.filesystem_mapping = transfer_objects["filesystems"]

    # TODO: per-job concurrency depending on the filesystem being operated on
    job.max_concurrency = min(fs.max_concurrency for fs in transfer_objects["filesystems"].values())

    log(f"State:            {config['stateroot']}")
    log(f"Job ID:           {job.job_id}")
    log(f"Max Attempts:     {job.max_attempts}")
    log(f"Max Concurrency:  {job.max_concurrency}")

    tasks = 0
    if "source" in config.keys():
        files = transfer_objects["filesystems"][config["source"]]._identify_by_fofn(T.Path(config["fofn"]))

    elif "route" in config.keys():
        _filesystem = transfer_objects["named_routes"][config["route"]].source()
        files = _filesystem._identify_by_fofn(T.Path(config["fofn"]))

    for task in transfer.plan(files):
        log(("=" if tasks == 0 else "-") * 72)

        job += task
        tasks += 1

        log(f"Source: {task.source.filesystem} {task.source.address}")
        log(f"Target: {task.target.filesystem} {task.target.address}")

    log("=" * 72)
    log(f"Added {tasks} tasks to job:")
    _print_status(job.status)

    binary = T.Path( config["command"][0] ).resolve()
    arguments = config["command"][1:]

    v_indices = [i for i,val in enumerate(arguments) if val=="-v"]
    variables = ""
    for v in v_indices:
        variables += f"-v {arguments[v+1]}"

    lsf = transfer_objects["executor"]
    lsf_options = transfer_objects["phases"]["transfer"]

    concurrent_job = Job(f'"{binary}" {variables} --configuration {config["configuration"]} _exec --stateroot {config["stateroot"]} --job_id {job.job_id}')

    concurrent_job.workers = job.max_concurrency
    concurrent_job.stdout = concurrent_job.stderr = config["stateroot"] / "run.%I.log"

    runners = list(lsf.submit(concurrent_job, lsf_options))

    log(f"Execution job submitted with ID {runners[0].job} and {len(runners)} workers")

def run_state(config:T.Dict[str, T.Any]) -> None:
    """Iterates through the tasks in the state database and executes them."""

    transfer_objects = read_yaml(config["configuration"], config["variables"])

    job = NativeJob(config["stateroot"], job_id=config["job_id"], force_restart=True)

    job.filesystem_mapping = transfer_objects["filesystems"]

    log(f"State:        {config['stateroot']}")
    log(f"Job ID:       {job.job_id}")
    log(f"Max Attempts: {job.max_attempts}")

    lsf = transfer_objects["executor"]

    job.worker_index = worker_index = lsf.worker.id.worker
    log(f"Worker        {worker_index} of {job.max_concurrency}")

    try:
        _print_status(job.status)
    except WorkerRedundant:
        log("Worker has nothing to do, terminating.")
        exit(0)

    tasks = 0
    while job.status:
        try:
            task = next(job)

        except StopIteration:
            break

        except DataNotReady:
            log("Not ready, waiting 60 seconds...")
            sleep(60)
            continue

        log(("=" if tasks == 0 else "-") * 72)
        log(f"Source: {task.source.filesystem} {task.source.address}")
        log(f"Target: {task.target.filesystem} {task.target.address}")

        # TODO: check exit status and update state?
        task()

        tasks += 1

    log("=" * 72)
    log("Done.")
    _print_status(job.status)
