import socket

import asyncpg
import pytest
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError

from paystream.db.database import database_unavailable


def wrapped(error: Exception, kind: type[DBAPIError] = DBAPIError, *, invalidated: bool = False) -> DBAPIError:
    """An asyncpg error the way SQLAlchemy raises it; the classes are those a probe against Postgres 18 saw."""
    return kind("select 1", None, AsyncAdapt_asyncpg_dbapi.Error(str(error), error), connection_invalidated=invalidated)


@pytest.mark.parametrize(
    ("error", "unavailable"),
    [
        (ConnectionRefusedError(61, "Connect call failed"), True),  # nothing listens on the port
        (socket.gaierror(8, "nodename nor servname provided"), True),  # the host is gone: a stopped compose service
        (wrapped(asyncpg.ConnectionDoesNotExistError("closed in the middle of operation"), invalidated=True), True),
        (wrapped(asyncpg.CannotConnectNowError("the database system is shutting down")), True),
        (wrapped(asyncpg.UniqueViolationError("duplicate key value"), IntegrityError), False),  # the database answered
        (wrapped(asyncpg.InterfaceError("another operation is in progress"), InterfaceError), False),  # misuse
        (ValueError("not the database"), False),
    ],
    ids=["refused", "unresolvable", "connection-lost", "shutting-down", "constraint", "client-misuse", "other"],
)
def test_only_an_unreachable_database_is_unavailable(error: Exception, unavailable: bool) -> None:
    assert database_unavailable(error) is unavailable
