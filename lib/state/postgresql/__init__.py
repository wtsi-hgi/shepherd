from .db import PostgreSQL
from .state import PGPhaseStatus as PhaseStatus, \
                   PGJobStatus as JobStatus, \
                   PGAttempt as Attempt, \
                   PGJob as Job


from common.models.api import API, RequiredArgument

__protocol__ = API(
    callable = PostgreSQL,
    arguments = [
        RequiredArgument("database", str, help="Database name"),
        RequiredArgument("user", str, help="Username"),
        RequiredArgument("password", str, help="Password"),
        RequiredArgument("host", str, help="PostgreSQL host"),
        RequiredArgument("port", int, default=5432, help="PostgreSQL port")
    ],
    help="PostgreSQL state protocol"
)
