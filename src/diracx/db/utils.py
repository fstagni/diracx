from __future__ import annotations

__all__ = ("utcnow", "Column", "NullColumn", "DateNowColumn", "BaseDB")

from abc import ABCMeta
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import TYPE_CHECKING

from sqlalchemy import Column as RawColumn
from sqlalchemy import DateTime, Enum, MetaData
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine


class utcnow(expression.FunctionElement):
    type: TypeEngine = DateTime()
    inherit_cache: bool = True


@compiles(utcnow, "postgresql")
def pg_utcnow(element, compiler, **kw) -> str:
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(utcnow, "mssql")
def ms_utcnow(element, compiler, **kw) -> str:
    return "GETUTCDATE()"


@compiles(utcnow, "mysql")
def mysql_utcnow(element, compiler, **kw) -> str:
    return "(UTC_TIMESTAMP)"


@compiles(utcnow, "sqlite")
def sqlite_utcnow(element, compiler, **kw) -> str:
    return "DATETIME('now')"


def substract_date(**kwargs: float) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(**kwargs)


Column = partial(RawColumn, nullable=False)
NullColumn = partial(RawColumn, nullable=True)
DateNowColumn = partial(Column, DateTime(timezone=True), server_default=utcnow())


def EnumColumn(enum_type, **kwargs):
    return Column(Enum(enum_type, native_enum=False, length=16), **kwargs)


class BaseDB(metaclass=ABCMeta):
    engine: AsyncEngine
    metadata: MetaData

    def __init__(self) -> None:
        self._conn = None

    @classmethod
    async def make_engine(cls, db_url: str) -> None:
        """TODO make metadata an abstract property"""
        cls.engine = create_async_engine(
            db_url,
            echo=True,
        )
        async with cls.engine.begin() as conn:
            await conn.run_sync(cls.metadata.create_all)

    @classmethod
    async def destroy_engine(cls) -> None:
        await cls.engine.dispose()

    @property
    def conn(self) -> AsyncConnection:
        if self._conn is None:
            raise RuntimeError(f"{self.__class__} was used before entering")
        return self._conn

    async def __aenter__(self):
        self._conn = await self.engine.connect().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._conn.commit()
        await self._conn.__aexit__(exc_type, exc, tb)
        self._conn = None
