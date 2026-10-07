from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


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
