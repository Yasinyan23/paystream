from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

# SQLSTATE class 08 (connection exception), and a server that is shutting down, crashed or not yet accepting work.
_UNAVAILABLE_SQLSTATES = ("08", "57P01", "57P02", "57P03")


class Database:
    """Owns the engine; every unit of work opens its own short session from `sessionmaker`."""

    def __init__(self, url: str, *, echo: bool = False) -> None:
        self.engine: AsyncEngine = create_async_engine(url, echo=echo, pool_pre_ping=True)
        self.sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(self.engine, expire_on_commit=False)

    async def ping(self) -> None:
        async with self.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        await self.engine.dispose()


def database_unavailable(exc: BaseException) -> bool:
    """The database could not be reached or dropped the connection: an outage, not a statement it rejected.

    As SQLAlchemy 2.1 with asyncpg raises them (probed against Postgres 18): a connection that cannot be opened, to a
    closed port or an unknown host, is a bare OSError; one lost in the middle of a statement, a DBAPIError marked
    `connection_invalidated`; a server that is shutting down or starting up refuses new connections with SQLSTATE
    57P03, a DBAPIError without that mark. OperationalError is what other drivers raise for the same.
    """
    if isinstance(exc, OSError | OperationalError):
        return True
    if not isinstance(exc, DBAPIError):
        return False
    sqlstate: str = getattr(exc.orig, "sqlstate", None) or ""
    return exc.connection_invalidated or sqlstate.startswith(_UNAVAILABLE_SQLSTATES)
